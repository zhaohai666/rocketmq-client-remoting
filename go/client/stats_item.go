// The model is a DIFFERENCE WINDOW, not "one bucket per period":
//
//	list = [seed(now-period, 0, 0), sample(t1, ...), sample(t2, ...), ...]
//	sum   = last.value - first.value
//	tps   = sum * 1000 / (last.timestamp - first.timestamp)
//	times = last.times - first.times
//	avgpt = sum / times                     (only when times > 0)
//
// so a snapshot spans whatever the list happens to retain — at most six real
// samples after the trim, i.e. <= 60s for the minute window — and it is NOT
// "the last whole minute". `times` is likewise a DIFFERENCE, not the raw
// counter. Both are easy to misread and produce plausible-looking but wrong
// numbers, so consumer_stats_test.go pins them against hand-computed values.
package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	statsMinuteListMax = 7
	statsHourListMax   = 7
	statsDayListMax    = 25
)

type callSnapshot struct {
	timestamp int64
	times     int64
	value     int64
}

// statsSnapshot mirrors common.stats.StatsSnapshot. Times holds timesDiff, not
// the raw counter.
type statsSnapshot struct {
	Sum   int64
	Tps   float64
	Avgpt float64
	Times int64
}

// statsItem mirrors common.stats.StatsItem (and RTStatsItem, whose only
// difference is the print format).
type statsItem struct {
	statsName string
	statsKey  string
	// rtItem selects RTStatsItem's print variant and nothing else.
	rtItem bool

	value      atomic.Int64
	times      atomic.Int64
	lastUpdate atomic.Int64

	mu           sync.Mutex
	csListMinute []callSnapshot
	csListHour   []callSnapshot
	csListDay    []callSnapshot
}

func newStatsItem(statsName, statsKey string, rtItem bool) *statsItem {
	it := &statsItem{statsName: statsName, statsKey: statsKey, rtItem: rtItem}
	it.lastUpdate.Store(common.CurrentTimeMillis())
	return it
}

func (s *statsItem) add(value, times int64) {
	s.value.Add(value)
	s.times.Add(times)
	s.lastUpdate.Store(common.CurrentTimeMillis())
}

// LastUpdateTimestamp feeds StatsItemSet#cleanResource (the idle-item sweep).
func (s *statsItem) LastUpdateTimestamp() int64 { return s.lastUpdate.Load() }

func computeStatsData(list []callSnapshot) statsSnapshot {
	var snap statsSnapshot
	if len(list) == 0 {
		return snap
	}
	first := list[0]
	last := list[len(list)-1]
	snap.Sum = last.value - first.value
	snap.Tps = float64(snap.Sum) * 1000.0 / float64(last.timestamp-first.timestamp)
	snap.Times = last.times - first.times
	if snap.Times > 0 {
		snap.Avgpt = float64(snap.Sum) / float64(snap.Times)
	}
	return snap
}

// sample appends one sample to a list, seeding it first when it is empty.
//
// The seed is what makes the FIRST snapshot meaningful: without it the first
// sample would be both first and last (span 0) and the TPS would be NaN.
func (s *statsItem) sample(list *[]callSnapshot, seedAgoMillis int64, maxLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := common.CurrentTimeMillis()
	if len(*list) == 0 {
		*list = append(*list, callSnapshot{timestamp: now - seedAgoMillis})
	}
	*list = append(*list, callSnapshot{timestamp: now, times: s.times.Load(), value: s.value.Load()})
	if len(*list) > maxLen {
		*list = append((*list)[:0], (*list)[1:]...)
	}
}

func (s *statsItem) samplingInSeconds() { s.sample(&s.csListMinute, 10*1000, statsMinuteListMax) }

func (s *statsItem) samplingInMinutes() { s.sample(&s.csListHour, 10*60*1000, statsHourListMax) }

func (s *statsItem) samplingInHour() { s.sample(&s.csListDay, 60*60*1000, statsDayListMax) }

func (s *statsItem) statsDataInMinute() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return computeStatsData(s.csListMinute)
}

func (s *statsItem) statsDataInHour() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return computeStatsData(s.csListHour)
}

func (s *statsItem) statsDataInDay() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return computeStatsData(s.csListDay)
}

func (s *statsItem) printAtMinutes() {
	common.LogInfof("[%s] [%s] Stats In One Minute, %s", s.statsName, s.statsKey, s.statPrintDetail(s.statsDataInMinute()))
}

func (s *statsItem) printAtHour() {
	common.LogInfof("[%s] [%s] Stats In One Hour, %s", s.statsName, s.statsKey, s.statPrintDetail(s.statsDataInHour()))
}

func (s *statsItem) printAtDay() {
	common.LogInfof("[%s] [%s] Stats In One Day, %s", s.statsName, s.statsKey, s.statPrintDetail(s.statsDataInDay()))
}

// statPrintDetail is StatsItem#statPrintDetail / RTStatsItem#statPrintDetail
// (the RT variant reports TIMES/AVGRT — for a response-time item the TPS and
// the sum are meaningless).
func (s *statsItem) statPrintDetail(snap statsSnapshot) string {
	if s.rtItem {
		return fmt.Sprintf("TIMES: %d AVGRT: %.2f", snap.Times, snap.Avgpt)
	}
	return fmt.Sprintf("SUM: %d TPS: %.2f AVGPT: %.2f", snap.Sum, snap.Tps, snap.Avgpt)
}

// ------------------------------------------------------------- StatsItemSet

// statsItemSet mirrors common.stats.StatsItemSet: a keyed table of statsItems
// sharing one statsName.
type statsItemSet struct {
	statsName string

	mu    sync.Mutex
	items map[string]*statsItem
}

func newStatsItemSet(statsName string) *statsItemSet {
	return &statsItemSet{statsName: statsName, items: map[string]*statsItem{}}
}

func (s *statsItemSet) addValue(key string, value, times int64) {
	s.getAndCreate(key, false).add(value, times)
}

func (s *statsItemSet) addRTValue(key string, value, times int64) {
	s.getAndCreate(key, true).add(value, times)
}

func (s *statsItemSet) getAndCreate(key string, rtItem bool) *statsItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[key]; ok {
		return it
	}
	it := newStatsItem(s.statsName, key, rtItem)
	s.items[key] = it
	return it
}

func (s *statsItemSet) getItem(key string) *statsItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[key]
}

func (s *statsItemSet) statsDataInMinute(key string) statsSnapshot {
	if it := s.getItem(key); it != nil {
		return it.statsDataInMinute()
	}
	return statsSnapshot{}
}

func (s *statsItemSet) statsDataInHour(key string) statsSnapshot {
	if it := s.getItem(key); it != nil {
		return it.statsDataInHour()
	}
	return statsSnapshot{}
}

func (s *statsItemSet) statsDataInDay(key string) statsSnapshot {
	if it := s.getItem(key); it != nil {
		return it.statsDataInDay()
	}
	return statsSnapshot{}
}

func (s *statsItemSet) snapshotItems() []*statsItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*statsItem, 0, len(s.items))
	for _, it := range s.items {
		out = append(out, it)
	}
	return out
}

func (s *statsItemSet) itemCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *statsItemSet) samplingInSeconds() {
	for _, it := range s.snapshotItems() {
		it.samplingInSeconds()
	}
}

func (s *statsItemSet) samplingInMinutes() {
	for _, it := range s.snapshotItems() {
		it.samplingInMinutes()
	}
}

func (s *statsItemSet) samplingInHour() {
	for _, it := range s.snapshotItems() {
		it.samplingInHour()
	}
}

func (s *statsItemSet) printAtMinutes() {
	for _, it := range s.snapshotItems() {
		it.printAtMinutes()
	}
}

func (s *statsItemSet) printAtHour() {
	for _, it := range s.snapshotItems() {
		it.printAtHour()
	}
}

func (s *statsItemSet) printAtDay() {
	for _, it := range s.snapshotItems() {
		it.printAtDay()
	}
}
