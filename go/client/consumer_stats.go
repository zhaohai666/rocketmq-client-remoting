// Consumer-side statistics (Java
// org.apache.rocketmq.client.stat.ConsumerStatsManager at
// client/src/main/java/org/apache/rocketmq/client/stat/ConsumerStatsManager.java).
//
// Five counter sets, every key `topic + "@" + group` (topic FIRST — the reverse
// order silently produces a table nobody can join against the broker's view):
//
//	CONSUME_OK_TPS      value += msgs acked by the listener
//	CONSUME_FAILED_TPS  value += msgs the listener rejected
//	CONSUME_RT          value += listener wall time (ms), times += 1
//	PULL_TPS            value += msgs handed over by a FOUND pull/pop
//	PULL_RT             value += pull round-trip wall time (ms), times += 1
//
// The manager lives on the CLIENT INSTANCE (Java
// MQClientInstance.consumerStatsManager, read through
// DefaultMQPushConsumerImpl#getConsumerStatsManager) rather than on a consumer,
// because several consumers on one clientId share it and because the 307
// running-info answer reports it per subscribed topic.
//
// What it exists FOR: `consumeStatus` is the payload of every
// GET_CONSUMER_RUNNING_INFO(307) answer's statusTable, which is what
// `mqadmin consumerStatus` and the dashboard's consumer lag view read. Without
// it the 307 answer is structurally valid but every field reads 0.
package client

import (
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// The five set names, copied verbatim from Java ConsumerStatsManager:30-34.
// They appear in the periodic `[%s] [%s] Stats In ...` log lines, so a typo
// here is visible only in the log.
const (
	statsNameConsumeOKTPS     = "CONSUME_OK_TPS"
	statsNameConsumeFailedTPS = "CONSUME_FAILED_TPS"
	statsNameConsumeRT        = "CONSUME_RT"
	statsNamePullTPS          = "PULL_TPS"
	statsNamePullRT           = "PULL_RT"
)

type consumerStatsManager struct {
	topicAndGroupConsumeOKTPS     *statsItemSet
	topicAndGroupConsumeRT        *statsItemSet
	topicAndGroupConsumeFailedTPS *statsItemSet
	topicAndGroupPullTPS          *statsItemSet
	topicAndGroupPullRT           *statsItemSet
}

func newConsumerStatsManager() *consumerStatsManager {
	return &consumerStatsManager{
		topicAndGroupConsumeOKTPS:     newStatsItemSet(statsNameConsumeOKTPS),
		topicAndGroupConsumeRT:        newStatsItemSet(statsNameConsumeRT),
		topicAndGroupConsumeFailedTPS: newStatsItemSet(statsNameConsumeFailedTPS),
		topicAndGroupPullTPS:          newStatsItemSet(statsNamePullTPS),
		topicAndGroupPullRT:           newStatsItemSet(statsNamePullRT),
	}
}

// statsKey is Java's `topic + "@" + group`.
func statsKey(topic, group string) string { return topic + "@" + group }

// ------------------------------------------------------------ recorders
//
// Java narrows every argument to `int` on the way in (`(int) rt`,
// `(int) msgs`). These take int64 and do not narrow: the only difference shows
// up past 2^31 (24.8 days of RT, or 2.1 billion messages in one call), where
// Java wraps and this does not. Narrowing would be reproducing an overflow, not
// a semantic.

func (m *consumerStatsManager) incPullRT(group, topic string, rt int64) {
	m.topicAndGroupPullRT.addRTValue(statsKey(topic, group), rt, 1)
}

func (m *consumerStatsManager) incPullTPS(group, topic string, msgs int64) {
	m.topicAndGroupPullTPS.addValue(statsKey(topic, group), msgs, 1)
}

func (m *consumerStatsManager) incConsumeRT(group, topic string, rt int64) {
	m.topicAndGroupConsumeRT.addRTValue(statsKey(topic, group), rt, 1)
}

func (m *consumerStatsManager) incConsumeOKTPS(group, topic string, msgs int64) {
	m.topicAndGroupConsumeOKTPS.addValue(statsKey(topic, group), msgs, 1)
}

func (m *consumerStatsManager) incConsumeFailedTPS(group, topic string, msgs int64) {
	m.topicAndGroupConsumeFailedTPS.addValue(statsKey(topic, group), msgs, 1)
}

// ------------------------------------------------------------- reporting

// consumeStatus is Java ConsumerStatsManager#consumeStatus:83-128 — the
// payload of the 307 answer's statusTable.
//
// Every field is a rate or an average, never a counter, and every one is read
// from the MINUTE window except consumeFailedMsgs, which is the HOUR window's
// sum. Do not "unify" that: the dashboard's failed-message column is meant to
// be the last hour, because a failure that happened one minute ago must not
// vanish from the view one minute later.
func (m *consumerStatsManager) consumeStatus(group, topic string) *remoting.ConsumeStatus {
	key := statsKey(topic, group)
	return &remoting.ConsumeStatus{
		PullRT:            m.topicAndGroupPullRT.statsDataInMinute(key).Avgpt,
		PullTPS:           m.topicAndGroupPullTPS.statsDataInMinute(key).Tps,
		ConsumeRT:         m.consumeRTAvg(group, topic),
		ConsumeOKTPS:      m.topicAndGroupConsumeOKTPS.statsDataInMinute(key).Tps,
		ConsumeFailedTPS:  m.topicAndGroupConsumeFailedTPS.statsDataInMinute(key).Tps,
		ConsumeFailedMsgs: m.topicAndGroupConsumeFailedTPS.statsDataInHour(key).Sum,
	}
}

// consumeRTAvg is Java ConsumerStatsManager#getConsumeRT:138-145, the ONE field
// with a fallback: when the minute window holds nothing (sum == 0) the average
// comes from the hour window instead. A consumer that has not consumed for a
// minute would otherwise report 0 ms RT, which reads as "instant" rather than
// "no data" — and PullRT/FailedTPS have no such fallback, so the fallback must
// stay confined to this field.
func (m *consumerStatsManager) consumeRTAvg(group, topic string) float64 {
	key := statsKey(topic, group)
	snap := m.topicAndGroupConsumeRT.statsDataInMinute(key)
	if snap.Sum == 0 {
		snap = m.topicAndGroupConsumeRT.statsDataInHour(key)
	}
	return snap.Avgpt
}

// ------------------------------------------------------------- scheduling

func (m *consumerStatsManager) sets() []*statsItemSet {
	return []*statsItemSet{
		m.topicAndGroupConsumeOKTPS,
		m.topicAndGroupConsumeRT,
		m.topicAndGroupConsumeFailedTPS,
		m.topicAndGroupPullTPS,
		m.topicAndGroupPullRT,
	}
}

// samplingInSeconds / samplingInMinutes / samplingInHour drive one cadence over
// every set. Separate from schedule() so the unit guards can advance time by
// hand: a real 10-minute window cannot be waited for in a test, and the whole
// point of the difference-window arithmetic is only visible across two samples.
func (m *consumerStatsManager) samplingInSeconds() {
	for _, set := range m.sets() {
		set.samplingInSeconds()
	}
}

func (m *consumerStatsManager) samplingInMinutes() {
	for _, set := range m.sets() {
		set.samplingInMinutes()
	}
}

func (m *consumerStatsManager) samplingInHour() {
	for _, set := range m.sets() {
		set.samplingInHour()
	}
}

func (m *consumerStatsManager) printAtMinutes() {
	for _, set := range m.sets() {
		set.printAtMinutes()
	}
}

func (m *consumerStatsManager) printAtHour() {
	for _, set := range m.sets() {
		set.printAtHour()
	}
}

func (m *consumerStatsManager) printAtDay() {
	for _, set := range m.sets() {
		set.printAtDay()
	}
}

// Stats-sampling cadences (Java StatsItemSet#init:50-108). Java attaches these
// six tasks to MQClientInstance's ScheduledExecutorService once per SET, i.e.
// thirty tasks for the five sets; concentrating the same six cadences on the
// manager is equivalent (each set is walked at the same instants) and five
// times cheaper.
const (
	statsSecondsPeriodMillis = 10 * 1000
	statsMinutesPeriodMillis = 10 * 60 * 1000
	statsHourPeriodMillis    = 60 * 60 * 1000
	statsDayPeriodMillis     = 24 * 60 * 60 * 1000
)

// schedule registers the six periodic tasks on the client instance. Java does
// this from the StatsItemSet constructor (the sets are built in
// MQClientInstance's own constructor) and ConsumerStatsManager#start is empty —
// so creation alone starts the sampler, and shutdown only ends it because
// MQClientInstance shuts the scheduler down. Here it hangs off Instance.Start
// so the goroutines stop with the instance's own stop channel.
func (m *consumerStatsManager) schedule(i *Instance) {
	i.spawnPeriodic(func() { m.samplingInSeconds() }, statsSecondsPeriodMillis, statsSecondsPeriodMillis)
	i.spawnPeriodic(func() { m.samplingInMinutes() }, 0, statsMinutesPeriodMillis)
	i.spawnPeriodic(func() { m.samplingInHour() }, 0, statsHourPeriodMillis)
	// The print tasks are anchored to the NEXT clock boundary (Java
	// UtilAll.computeNextMinutesTimeMillis and friends), not to the start
	// instant, so "Stats In One Minute" lines land on the minute.
	i.spawnPeriodic(func() { m.printAtMinutes() }, millisUntilNextMinute(), 60*1000)
	i.spawnPeriodic(func() { m.printAtHour() }, millisUntilNextHour(), statsHourPeriodMillis)
	i.spawnPeriodic(func() { m.printAtDay() }, millisUntilNextMorning()-2000, statsDayPeriodMillis)
}

func millisUntilNextMinute() int64 {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location()).
		Add(time.Minute)
	return next.Sub(now).Milliseconds()
}

func millisUntilNextHour() int64 {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location()).
		Add(time.Hour)
	return next.Sub(now).Milliseconds()
}

// millisUntilNextMorning is Java UtilAll#computeNextMorningTimeMillis: the next
// LOCAL midnight. Java subtracts 2s from the initial delay before scheduling
// the day print, which the caller applies.
func millisUntilNextMorning() int64 {
	now := time.Now()
	y, mo, d := now.Date()
	next := time.Date(y, mo, d, 0, 0, 0, 0, now.Location()).AddDate(0, 0, 1)
	return next.Sub(now).Milliseconds()
}

// ------------------------------------------------ consumer-side plumbing

// statsManager is the instance's manager. Nil before Start or after Shutdown,
// and nil for a consumer built without an instance (a unit test), so every
// recorder below is a no-op instead of a nil dereference.
func (c *DefaultMQPushConsumer) statsManager() *consumerStatsManager {
	inst := c.instance
	if inst == nil {
		return nil
	}
	return inst.consumerStats
}

func (c *DefaultMQPushConsumer) incPullRT(topic string, rt int64) {
	if m := c.statsManager(); m != nil {
		m.incPullRT(c.consumerGroup, topic, rt)
	}
}

func (c *DefaultMQPushConsumer) incPullTPS(topic string, msgs int64) {
	if m := c.statsManager(); m != nil {
		m.incPullTPS(c.consumerGroup, topic, msgs)
	}
}

func (c *DefaultMQPushConsumer) incConsumeRT(topic string, rt int64) {
	if m := c.statsManager(); m != nil {
		m.incConsumeRT(c.consumerGroup, topic, rt)
	}
}

func (c *DefaultMQPushConsumer) incConsumeOKTPS(topic string, msgs int64) {
	if m := c.statsManager(); m != nil {
		m.incConsumeOKTPS(c.consumerGroup, topic, msgs)
	}
}

func (c *DefaultMQPushConsumer) incConsumeFailedTPS(topic string, msgs int64) {
	if m := c.statsManager(); m != nil {
		m.incConsumeFailedTPS(c.consumerGroup, topic, msgs)
	}
}

// recordConsumeSuccessTPS is Java's `case CONSUME_SUCCESS` arm of
// processConsumeResult (ConsumeMessageConcurrentlyService:213-221 and
// ConsumeMessagePopConcurrentlyService:187-195): [0, ackIndex] count as OK and
// the remainder as failed.
//
// `ackIndex` must be the value ALREADY clamped to len(batch)-1. The failed
// counter is bumped even when it is zero — Java does, and although a zero adds
// nothing to the value it does add a `times` tick that the hour-window
// consumeFailedMsgs sum is computed over.
//
// A listener that returns CONSUME_SUCCESS with a negative AckIndex makes `ok`
// negative here, exactly as in Java. That is not clamped away on purpose:
// clamping would change what the 307 answer reports versus the reference
// client. (The listener contract says ackIndex >= 0; only a bug reaches it.)
func (c *DefaultMQPushConsumer) recordConsumeSuccessTPS(topic string, ackIndex, size int) {
	ok := ackIndex + 1
	c.incConsumeOKTPS(topic, int64(ok))
	c.incConsumeFailedTPS(topic, int64(size-ok))
}
