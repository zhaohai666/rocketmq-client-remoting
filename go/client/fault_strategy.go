package client

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Send latency fault tolerance (Java org.apache.rocketmq.client.latency.*,
// Python client/latency.py).
//
// MQFaultStrategy + LatencyFaultToleranceImpl track each broker's send latency
// and ISOLATE a broker for a while after a slow send or an exception, so new
// messages stop being routed to it. Off by default (sendLatencyFaultEnable),
// exactly like Java.
//
// Java's background reachability detector (startDetector) is ported in
// fault_detector.go: when startDetectorEnable is on, a producer-owned goroutine
// periodically probes every broker in the fault table (GET_MAX_OFFSET, the
// ServiceDetector Java wires in DefaultMQProducerImpl) and flips a broker that
// answers back from unreachable to reachable. reachableFlag keeps its Java
// semantics — send failures write it, the detector restores it — and the
// reachable filter still behaves correctly without the detector because a
// broker with no entry is considered reachable.

// faultItem is one broker's fault record (Java
// LatencyFaultToleranceImpl.FaultItem). checkStamp schedules the detector: the
// broker is probed once its checkStamp has come due, then it is pushed
// detectInterval into the future.
type faultItem struct {
	name           string
	currentLatency float64
	startTimestamp float64
	checkStamp     float64
	reachableFlag  bool
}

func newFaultItem(name string) *faultItem {
	return &faultItem{name: name, reachableFlag: true}
}

// updateNotAvailableDuration extends the isolation window, but only when the
// new deadline is LATER than the current one (Java: keep the longest window;
// a short failure must not shorten a long isolation).
func (f *faultItem) updateNotAvailableDuration(notAvailableDuration float64) {
	now := float64(time.Now().UnixMilli())
	if notAvailableDuration > 0 && now+notAvailableDuration > f.startTimestamp {
		f.startTimestamp = now + notAvailableDuration
	}
}

func (f *faultItem) isAvailable() bool {
	return float64(time.Now().UnixMilli()) >= f.startTimestamp
}

func (f *faultItem) isReachable() bool { return f.reachableFlag }

// latencyFaultToleranceImpl is the in-memory fault table (Java
// LatencyFaultToleranceImpl). The detector half — detectTimeout/detectInterval/
// startDetectorEnable, the Resolver and the ServiceDetector — is driven by the
// producer's detectLoop in fault_detector.go.
type latencyFaultToleranceImpl struct {
	mu    sync.Mutex
	items map[string]*faultItem

	detectTimeout       float64 // ms, Java ClientConfig default 200
	detectInterval      float64 // ms per broker, Java ClientConfig default 2000
	startDetectorEnable bool
	tickMillis          atomic.Int64 // scheduler tick, Java's fixed 3s scheduleAtFixedRate

	// resolver maps a broker name to its master publish address (Java
	// Resolver = DefaultMQProducerImpl::findBrokerAddressInPublish); detector
	// is the ServiceDetector (GET_MAX_OFFSET). Both are installed by Start and
	// nil until then.
	resolver func(name string) (addr string, ok bool)
	detector func(addr string, timeoutMillis int64) bool
}

// Java ClientConfig's detector defaults.
const (
	defaultDetectTimeout  = float64(200)
	defaultDetectInterval = float64(2 * 1000)
	detectorTickMillis    = int64(3 * 1000) // Java startDetector's fixed 3s tick
)

func newLatencyFaultToleranceImpl() *latencyFaultToleranceImpl {
	tol := &latencyFaultToleranceImpl{
		items:          map[string]*faultItem{},
		detectTimeout:  defaultDetectTimeout,
		detectInterval: defaultDetectInterval,
	}
	tol.tickMillis.Store(detectorTickMillis)
	return tol
}

// setDetector installs the Resolver + ServiceDetector pair. Called once by
// producer Start, once the client instance exists to resolve against.
func (l *latencyFaultToleranceImpl) setDetector(resolver func(string) (string, bool), detector func(string, int64) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resolver = resolver
	l.detector = detector
}

func (l *latencyFaultToleranceImpl) setStartDetectorEnable(enable bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.startDetectorEnable = enable
}

func (l *latencyFaultToleranceImpl) isStartDetectorEnable() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.startDetectorEnable
}

func (l *latencyFaultToleranceImpl) setDetectTimeout(millis int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.detectTimeout = float64(millis)
}

func (l *latencyFaultToleranceImpl) setDetectInterval(millis int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.detectInterval = float64(millis)
}

// detectByOneRound is Java LatencyFaultToleranceImpl.detectByOneRound, line by
// line: for every fault item whose checkStamp is due, advance the stamp by
// detectInterval, resolve the broker's master address, and probe it. A broker
// the route no longer knows loses its fault record (unknown = available); a
// successful probe flips an unreachable broker back to reachable. A FAILED
// probe changes nothing — Java only ever clears the flag here, it never sets
// it; marking unreachable is the send path's job.
//
// The table lock is held only for the snapshot and the write-back, never
// across the network call: one probe may block for detectTimeout.
func (l *latencyFaultToleranceImpl) detectByOneRound() {
	l.mu.Lock()
	now := float64(time.Now().UnixMilli())
	due := make([]*faultItem, 0, len(l.items))
	for _, item := range l.items {
		if now < item.checkStamp {
			continue
		}
		item.checkStamp = now + l.detectInterval
		due = append(due, item)
	}
	resolver := l.resolver
	detector := l.detector
	timeout := int64(l.detectTimeout)
	l.mu.Unlock()

	for _, item := range due {
		if resolver == nil {
			return
		}
		addr, ok := resolver(item.name)
		if !ok {
			// Java: the resolver returning null removes the item — the broker
			// has left the route, so its fault record must not outlive it.
			l.remove(item.name)
			continue
		}
		if detector == nil {
			continue
		}
		if detector(addr, timeout) {
			l.mu.Lock()
			if !item.reachableFlag {
				common.LogInfof("%s is reachable now, then it can be used.", item.name)
				item.reachableFlag = true
			}
			l.mu.Unlock()
		}
	}
}

// detectByOneRoundIfEnabled gates the round on startDetectorEnable, exactly the
// check Java's scheduled runnable makes every tick.
func (l *latencyFaultToleranceImpl) detectByOneRoundIfEnabled() {
	if !l.isStartDetectorEnable() {
		return
	}
	l.detectByOneRound()
}

func (l *latencyFaultToleranceImpl) updateFaultItem(name string, currentLatency, notAvailableDuration float64, reachable bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	item, ok := l.items[name]
	if !ok {
		item = newFaultItem(name)
		l.items[name] = item
	}
	item.currentLatency = currentLatency
	item.updateNotAvailableDuration(notAvailableDuration)
	item.reachableFlag = reachable
}

// isAvailable / isReachable both answer TRUE for an unknown broker (Java: a
// broker that never failed is available and reachable). The item's fields are
// read under the table lock: the detector loop writes reachableFlag /
// startTimestamp from its own goroutine, so an unlocked read would race.
func (l *latencyFaultToleranceImpl) isAvailable(name string) bool {
	l.mu.Lock()
	item, ok := l.items[name]
	available := true
	if ok {
		available = float64(time.Now().UnixMilli()) >= item.startTimestamp
	}
	l.mu.Unlock()
	return available
}

func (l *latencyFaultToleranceImpl) isReachable(name string) bool {
	l.mu.Lock()
	item, ok := l.items[name]
	reachable := true
	if ok {
		reachable = item.reachableFlag
	}
	l.mu.Unlock()
	return reachable
}

func (l *latencyFaultToleranceImpl) remove(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.items, name)
}

func (l *latencyFaultToleranceImpl) faultItem(name string) *faultItem {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.items[name]
}

// mqFaultStrategy mirrors Java client.latency.MQFaultStrategy.
//
// With sendLatencyFaultEnable off (the default) selection degrades to: try the
// round-robin ring while skipping lastBrokerName, and if a full round is
// filtered out, fall back to unconditional round-robin. Note there is
// deliberately no resetIndex here — Python's latency.py does the same, and the
// index reset is only needed by the enabled path to escape a poisoned cursor.
type mqFaultStrategy struct {
	mu                     sync.Mutex
	sendLatencyFaultEnable bool
	startDetectorEnable    bool
	latencyFaultTolerance  *latencyFaultToleranceImpl
	latencyMax             []float64
	notAvailableDuration   []float64
}

// Java MQFaultStrategy.LATENCY_MAX / NOT_AVAILABLE_DURATION.
var (
	defaultLatencyMax           = []float64{50, 100, 550, 1800, 3000, 5000, 15000}
	defaultNotAvailableDuration = []float64{0, 0, 2000, 5000, 6000, 10000, 30000}
)

func newMQFaultStrategy(sendLatencyFaultEnable bool) *mqFaultStrategy {
	return &mqFaultStrategy{
		sendLatencyFaultEnable: sendLatencyFaultEnable,
		latencyFaultTolerance:  newLatencyFaultToleranceImpl(),
		latencyMax:             append([]float64(nil), defaultLatencyMax...),
		notAvailableDuration:   append([]float64(nil), defaultNotAvailableDuration...),
	}
}

func (s *mqFaultStrategy) SendLatencyFaultEnable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendLatencyFaultEnable
}

func (s *mqFaultStrategy) SetSendLatencyFaultEnable(enable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendLatencyFaultEnable = enable
}

// StartDetectorEnable / SetStartDetectorEnable / SetDetectTimeout /
// SetDetectInterval are the Java MQFaultStrategy passthroughs onto the fault
// table (startDetectorEnable lives on both, like Java).
func (s *mqFaultStrategy) StartDetectorEnable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startDetectorEnable
}

func (s *mqFaultStrategy) SetStartDetectorEnable(enable bool) {
	s.mu.Lock()
	s.startDetectorEnable = enable
	tolerance := s.latencyFaultTolerance
	s.mu.Unlock()
	tolerance.setStartDetectorEnable(enable)
}

func (s *mqFaultStrategy) setDetectTimeout(millis int64) {
	s.latencyFaultTolerance.setDetectTimeout(millis)
}

func (s *mqFaultStrategy) setDetectInterval(millis int64) {
	s.latencyFaultTolerance.setDetectInterval(millis)
}

// selectOneMessageQueue picks a queue, optionally avoiding lastBrokerName.
// The caller treats ok == false as "no queue at all" and gives up the round.
func (s *mqFaultStrategy) selectOneMessageQueue(info *TopicPublishInfo, lastBrokerName string, resetIndex bool) (common.MessageQueue, bool, error) {
	brokerFilter := func(mq *common.MessageQueue) bool {
		return lastBrokerName == "" || mq.BrokerName != lastBrokerName
	}
	if s.SendLatencyFaultEnable() {
		if resetIndex {
			info.ResetIndex()
		}
		availableFilter := func(mq *common.MessageQueue) bool {
			return s.latencyFaultTolerance.isAvailable(mq.BrokerName)
		}
		reachableFilter := func(mq *common.MessageQueue) bool {
			return s.latencyFaultTolerance.isReachable(mq.BrokerName)
		}
		if mq, ok, err := info.SelectOneMessageQueue([]QueueFilter{availableFilter, brokerFilter}); err != nil || ok {
			return mq, ok, err
		}
		if mq, ok, err := info.SelectOneMessageQueue([]QueueFilter{reachableFilter, brokerFilter}); err != nil || ok {
			return mq, ok, err
		}
		return info.SelectOneMessageQueue(nil)
	}
	if mq, ok, err := info.SelectOneMessageQueue([]QueueFilter{brokerFilter}); err != nil || ok {
		return mq, ok, err
	}
	return info.SelectOneMessageQueue(nil)
}

// updateFaultItem records one attempt's outcome; a no-op while the strategy is
// disabled.
func (s *mqFaultStrategy) updateFaultItem(brokerName string, currentLatency float64, isolation, reachable bool) {
	if !s.SendLatencyFaultEnable() {
		return
	}
	latency := currentLatency
	if isolation {
		// An exception carries no latency: Java pins it to 10000ms so it lands
		// in the 10s isolation bucket.
		latency = 10000
	}
	duration := s.computeNotAvailableDuration(latency)
	s.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable)
}

// computeNotAvailableDuration walks the two tables from the top: the FIRST
// (highest) threshold the latency reaches decides the window.
func (s *mqFaultStrategy) computeNotAvailableDuration(currentLatency float64) float64 {
	for i := len(s.latencyMax) - 1; i >= 0; i-- {
		if currentLatency >= s.latencyMax[i] {
			return s.notAvailableDuration[i]
		}
	}
	return 0
}
