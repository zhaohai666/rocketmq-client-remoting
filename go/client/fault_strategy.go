package client

import (
	"sync"
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
// Java's background reachability detector (startDetector) is deliberately not
// ported: it opens real connections on a timer, and this port has no such
// thread anywhere else. reachableFlag keeps its semantics — it is only written
// by the explicit reachable argument of updateFaultItem — and the reachable
// filter still behaves correctly because a broker with no entry is considered
// reachable.

// faultItem is one broker's fault record (Java
// LatencyFaultToleranceImpl.FaultItem).
type faultItem struct {
	name           string
	currentLatency float64
	startTimestamp float64
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
// LatencyFaultToleranceImpl, minus the detector thread).
type latencyFaultToleranceImpl struct {
	mu    sync.Mutex
	items map[string]*faultItem
}

func newLatencyFaultToleranceImpl() *latencyFaultToleranceImpl {
	return &latencyFaultToleranceImpl{items: map[string]*faultItem{}}
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
// broker that never failed is available and reachable).
func (l *latencyFaultToleranceImpl) isAvailable(name string) bool {
	l.mu.Lock()
	item, ok := l.items[name]
	l.mu.Unlock()
	if ok {
		return item.isAvailable()
	}
	return true
}

func (l *latencyFaultToleranceImpl) isReachable(name string) bool {
	l.mu.Lock()
	item, ok := l.items[name]
	l.mu.Unlock()
	if ok {
		return item.isReachable()
	}
	return true
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
