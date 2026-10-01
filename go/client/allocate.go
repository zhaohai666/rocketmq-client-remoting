// Queue allocation strategies (Java
// org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueue*).
//
// Deliberate deviation from Java, shared by all four ports: Java's
// AbstractAllocateMessageQueueStrategy#check throws IllegalArgumentException
// on bad input; here every strategy returns an empty list instead. Rebalance is
// a background timer — one dirty input must not kill the consumer.
//
// Determinism is the whole point: every instance in a group sorts mqAll and
// cidAll the same way and runs the same index math, otherwise two instances
// compute overlapping assignments and duplicate every message.
package client

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// AllocateMessageQueueStrategy is the SPI.
type AllocateMessageQueueStrategy interface {
	// Allocate returns the queues this instance owns. Must not return nil for
	// a valid input with no queues — an empty slice is fine.
	Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue
	// Name is Java getName ("AVG", "CONSISTENT_HASH", ...).
	Name() string
}

// AllocateErrReporter is implemented by strategies that can fail hard —
// Java's AllocateMachineRoomNearBy throws IllegalArgumentException when the
// resolver hands out an empty machine room, the rebalance thread lets the
// exception abort the round and processQueueTable stays untouched. The Go
// interface has no error channel, so such a strategy records the failure and
// the rebalance call sites ask for it via this interface: a non-nil error
// KEEPS the previous assignment instead of revoking every queue (silently
// returning empty would take the whole topic away from the group).
type AllocateErrReporter interface {
	AllocateErr() error
}

func strategyName(s AllocateMessageQueueStrategy) string {
	if s == nil {
		return "<nil>"
	}
	return s.Name()
}

// checkCid reproduces Java AbstractAllocateMessageQueueStrategy#check: the
// currentCID must be IN cidAll, and Java logs the [BUG] line when it is not.
func checkCid(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) (int, bool) {
	if currentCID == "" || len(mqAll) == 0 || len(cidAll) == 0 {
		return 0, false
	}
	index := -1
	for i, cid := range cidAll {
		if cid == currentCID {
			index = i
			break
		}
	}
	if index < 0 {
		common.LogInfof("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %v", consumerGroup, currentCID, cidAll)
		return 0, false
	}
	return index, true
}

// ------------------------------------------------------------- AVG

// AllocateMessageQueueAveragely is Java AllocateMessageQueueAveragely.
type AllocateMessageQueueAveragely struct{}

func (AllocateMessageQueueAveragely) Name() string { return "AVG" }

func (AllocateMessageQueueAveragely) Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	index, ok := checkCid(consumerGroup, currentCID, mqAll, cidAll)
	if !ok {
		return nil
	}
	mod := len(mqAll) % len(cidAll)
	averageSize := len(mqAll) / len(cidAll)
	if averageSize == 0 {
		if index < len(mqAll) {
			return []common.MessageQueue{mqAll[index]}
		}
		return nil
	}
	var start, end int
	if mod > 0 && index < mod {
		start = index * (averageSize + 1)
		end = start + averageSize + 1
	} else {
		start = mod*(averageSize+1) + (index-mod)*averageSize
		end = start + averageSize
	}
	return append([]common.MessageQueue(nil), mqAll[start:end]...)
}

// ------------------------------------------------ AVG_BY_CIRCLE

// AllocateMessageQueueAveragelyByCircle is Java
// AllocateMessageQueueAveragelyByCircle.
type AllocateMessageQueueAveragelyByCircle struct{}

func (AllocateMessageQueueAveragelyByCircle) Name() string { return "AVG_BY_CIRCLE" }

func (AllocateMessageQueueAveragelyByCircle) Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	index, ok := checkCid(consumerGroup, currentCID, mqAll, cidAll)
	if !ok {
		return nil
	}
	var out []common.MessageQueue
	for i := index; i < len(mqAll); i += len(cidAll) {
		out = append(out, mqAll[i])
	}
	return out
}

// ------------------------------------------------------------- CONFIG

// AllocateMessageQueueByConfig is Java AllocateMessageQueueByConfig: whatever
// the caller configured. Note Java does NO check here — an empty group or
// cidAll still returns the configured list.
type AllocateMessageQueueByConfig struct {
	MessageQueueList []common.MessageQueue
}

// NewAllocateMessageQueueByConfig builds the strategy.
func NewAllocateMessageQueueByConfig(mqs []common.MessageQueue) *AllocateMessageQueueByConfig {
	return &AllocateMessageQueueByConfig{MessageQueueList: append([]common.MessageQueue(nil), mqs...)}
}

func (*AllocateMessageQueueByConfig) Name() string { return "CONFIG" }

// Allocate ignores every argument (Java does too — no check runs here).
func (s *AllocateMessageQueueByConfig) Allocate(_, _ string, _ []common.MessageQueue, _ []string) []common.MessageQueue {
	return append([]common.MessageQueue(nil), s.MessageQueueList...)
}

// ------------------------------------------------- consistent hash ring

// hashFunction is Java HashFunction.
type hashFunction interface {
	Hash(key string) uint32
}

// md5Hash is Java ConsistentHashRouter.MD5Hash: the FIRST FOUR BYTES of the MD5
// digest, big endian. Taking the full 128 bits (or a different slice) puts you
// on a different ring than every Java client.
type md5Hash struct{}

func (md5Hash) Hash(key string) uint32 {
	sum := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(sum[:4])
}

type virtualNode struct {
	key         string
	physicalKey string
}

type consistentHashRouter struct {
	hashFn hashFunction
	ring   map[uint32]virtualNode
	keys   []uint32
}

func newConsistentHashRouter(pNodes []string, vNodeCount int, fn hashFunction) *consistentHashRouter {
	if fn == nil {
		fn = md5Hash{}
	}
	r := &consistentHashRouter{hashFn: fn, ring: map[uint32]virtualNode{}}
	for _, node := range pNodes {
		r.addNode(node, vNodeCount)
	}
	return r
}

func (r *consistentHashRouter) addNode(pNode string, vNodeCount int) {
	existing := r.existingReplicas(pNode)
	for i := 0; i < vNodeCount; i++ {
		v := virtualNode{key: javaVirtualNodeKey(pNode, i+existing), physicalKey: pNode}
		key := r.hashFn.Hash(v.key)
		if _, ok := r.ring[key]; !ok {
			r.keys = insertU32(r.keys, key)
		}
		// Java TreeMap.put: a duplicate hash is overwritten in place.
		r.ring[key] = v
	}
}

func (r *consistentHashRouter) removeNode(pNode string) {
	kept := r.keys[:0]
	for _, key := range r.keys {
		if r.ring[key].physicalKey == pNode {
			delete(r.ring, key)
			continue
		}
		kept = append(kept, key)
	}
	r.keys = kept
}

// routeNode is Java ConsistentHashRouter#routeNode: tailMap(hash).firstKey(),
// and TreeMap's tailMap is INCLUSIVE of the endpoint, so the lookup is a
// "first key >= hash" search. Wrapping past the end returns the first key.
func (r *consistentHashRouter) routeNode(objectKey string) (string, bool) {
	if len(r.keys) == 0 {
		return "", false
	}
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= r.hashFn.Hash(objectKey) })
	if idx == len(r.keys) {
		idx = 0
	}
	return r.ring[r.keys[idx]].physicalKey, true
}

func (r *consistentHashRouter) existingReplicas(pNode string) int {
	n := 0
	for _, v := range r.ring {
		if v.physicalKey == pNode {
			n++
		}
	}
	return n
}

func javaVirtualNodeKey(pNode string, replicaIndex int) string {
	return pNode + "-" + itoa(replicaIndex)
}

func insertU32(sorted []uint32, v uint32) []uint32 {
	i := sort.Search(len(sorted), func(i int) bool { return sorted[i] >= v })
	sorted = append(sorted, 0)
	copy(sorted[i+1:], sorted[i:])
	sorted[i] = v
	return sorted
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// javaMessageQueueString is Java MessageQueue#toString — the string the ring
// hashes. Must match character for character.
func javaMessageQueueString(mq common.MessageQueue) string { return mq.JavaString() }

// ------------------------------------------- CONSISTENT_HASH

// AllocateMessageQueueConsistentHash is Java
// AllocateMessageQueueConsistentHash. Its point is not evenness but STABILITY:
// when the queue or consumer count changes, only the queues on the affected arc
// switch owner, while AVG moves everybody's boundary.
type AllocateMessageQueueConsistentHash struct {
	VirtualNodeCnt     int
	CustomHashFunction hashFunction
}

// NewAllocateMessageQueueConsistentHash mirrors the Java default: 10 virtual
// nodes, MD5Hash.
func NewAllocateMessageQueueConsistentHash(virtualNodeCnt int) *AllocateMessageQueueConsistentHash {
	return &AllocateMessageQueueConsistentHash{VirtualNodeCnt: virtualNodeCnt}
}

func (*AllocateMessageQueueConsistentHash) Name() string { return "CONSISTENT_HASH" }

func (s *AllocateMessageQueueConsistentHash) Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	if _, ok := checkCid(consumerGroup, currentCID, mqAll, cidAll); !ok {
		return nil
	}
	router := newConsistentHashRouter(cidAll, s.VirtualNodeCnt, s.CustomHashFunction)
	var out []common.MessageQueue
	for _, mq := range mqAll {
		node, ok := router.routeNode(javaMessageQueueString(mq))
		if ok && node == currentCID {
			out = append(out, mq)
		}
	}
	return out
}

// ------------------------------------------------- MACHINE_ROOM

// javaSplit mirrors Java String#split(sep) with limit 0: trailing empty
// segments are DROPPED, but a string with no separator is returned whole
// (Java's "if no match was found, return this" early return) — so "" stays [""]
// and does not become empty.
func javaSplit(text, sep string) []string {
	parts := strings.Split(text, sep)
	if len(parts) == 1 {
		return parts
	}
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// AllocateMessageQueueByMachineRoom is Java
// AllocateMessageQueueByMachineRoom. Broker names are expected to look like
// `<idc>@<brokerName>`; only queues whose idc prefix is in ConsumerIDCs take
// part, and those are then split by an AVG-like rule whose remainder goes to
// the FIRST `mod` consumers (java uses `rem > currentIndex`, not `>=`).
type AllocateMessageQueueByMachineRoom struct {
	ConsumerIDCs map[string]struct{}
}

// NewAllocateMessageQueueByMachineRoom builds the strategy.
func NewAllocateMessageQueueByMachineRoom(idcs []string) *AllocateMessageQueueByMachineRoom {
	set := map[string]struct{}{}
	for _, idc := range idcs {
		set[idc] = struct{}{}
	}
	return &AllocateMessageQueueByMachineRoom{ConsumerIDCs: set}
}

func (*AllocateMessageQueueByMachineRoom) Name() string { return "MACHINE_ROOM" }

func (s *AllocateMessageQueueByMachineRoom) Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	index, ok := checkCid(consumerGroup, currentCID, mqAll, cidAll)
	if !ok {
		return nil
	}
	// Java's consumeridcs has no default (a nil field NPEs on contains); this
	// port defaults to an empty set, i.e. "nothing is allocated".
	var candidates []common.MessageQueue
	for _, mq := range mqAll {
		parts := javaSplit(mq.BrokerName, "@")
		if len(parts) != 2 {
			continue
		}
		if _, hit := s.ConsumerIDCs[parts[0]]; !hit {
			continue
		}
		if parts[1] == "" {
			continue
		}
		candidates = append(candidates, mq)
	}
	rem := len(candidates) % len(cidAll)
	size := len(candidates) / len(cidAll)
	var out []common.MessageQueue
	start := index * size
	if rem > index {
		start += index
	} else {
		start += rem
	}
	end := start + size
	if rem > index {
		end++
	}
	for i := start; i < end; i++ {
		out = append(out, candidates[i])
	}
	return out
}

// ------------------------------------------------ MACHINE_ROOM_NEARBY

// MachineRoomResolver is Java AllocateMachineRoomNearBy.MachineRoomResolver:
// it tells the strategy which machine room a queue or a consumer lives in.
// Java's javadoc is explicit that neither method may return null/empty — an
// empty room aborts the allocation with an error (see AllocateErrReporter).
type MachineRoomResolver interface {
	BrokerDeployIn(mq common.MessageQueue) string
	ConsumerDeployIn(clientID string) string
}

// AllocateMachineRoomNearby is Java AllocateMachineRoomNearBy: a proxy around
// another strategy. Queues and consumers are grouped by machine room, then
//
//  1. the queues in THIS consumer's room are split among the consumers of that
//     room only (through the inner strategy);
//  2. queues in a room with NO alive consumer at all are shared among ALL
//     consumers (again through the inner strategy) — otherwise nobody would
//     consume them.
//
// getName() is "MACHINE_ROOM_NEARBY" + "-" + <inner name> (Java likewise), so
// logs can tell which algorithm actually did the splitting.
//
// Java throws NullPointerException when either constructor argument is null
// and IllegalArgumentException when the resolver returns an empty room; the
// constructor mirrors that as an error, and the per-allocate failure is
// reported through AllocateErrReporter so the rebalance round keeps the
// current assignment instead of revoking everything.
type AllocateMachineRoomNearby struct {
	Inner    AllocateMessageQueueStrategy
	Resolver MachineRoomResolver

	mu  sync.Mutex
	err error
}

// NewAllocateMachineRoomNearby mirrors Java's constructor null checks.
func NewAllocateMachineRoomNearby(inner AllocateMessageQueueStrategy, resolver MachineRoomResolver) (*AllocateMachineRoomNearby, error) {
	if inner == nil {
		return nil, errors.New("allocateMessageQueueStrategy is null")
	}
	if resolver == nil {
		return nil, errors.New("machineRoomResolver is null")
	}
	return &AllocateMachineRoomNearby{Inner: inner, Resolver: resolver}, nil
}

func (s *AllocateMachineRoomNearby) Name() string {
	return "MACHINE_ROOM_NEARBY-" + strategyName(s.Inner)
}

// AllocateErr exposes the failure recorded by the last Allocate call.
func (s *AllocateMachineRoomNearby) AllocateErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *AllocateMachineRoomNearby) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *AllocateMachineRoomNearby) Allocate(consumerGroup, currentCID string, mqAll []common.MessageQueue, cidAll []string) []common.MessageQueue {
	s.setErr(nil)
	if s.Inner == nil || s.Resolver == nil {
		s.setErr(errors.New("allocateMachineRoomNearby: inner strategy or resolver is nil"))
		return nil
	}
	if _, ok := checkCid(consumerGroup, currentCID, mqAll, cidAll); !ok {
		return nil
	}
	// Group queues by machine room. Java uses TreeMap, i.e. lexicographic room
	// order — keep it so the inner allocations are deterministic.
	mr2mq := map[string][]common.MessageQueue{}
	rooms := make([]string, 0, len(mqAll))
	for _, mq := range mqAll {
		room := s.Resolver.BrokerDeployIn(mq)
		if room == "" {
			err := fmt.Errorf("machine room is null for mq %s", mq.JavaString())
			s.setErr(err)
			common.LogErrorf("[MachineRoomNearby] %v", err)
			return nil
		}
		if _, seen := mr2mq[room]; !seen {
			rooms = append(rooms, room)
		}
		mr2mq[room] = append(mr2mq[room], mq)
	}
	sort.Strings(rooms)
	// Group consumers by machine room (same non-empty rule).
	mr2c := map[string][]string{}
	for _, cid := range cidAll {
		room := s.Resolver.ConsumerDeployIn(cid)
		if room == "" {
			err := fmt.Errorf("machine room is null for consumer id %s", cid)
			s.setErr(err)
			common.LogErrorf("[MachineRoomNearby] %v", err)
			return nil
		}
		mr2c[room] = append(mr2c[room], cid)
	}
	var out []common.MessageQueue
	// 1. This consumer's room: queues split among same-room consumers only.
	currentRoom := s.Resolver.ConsumerDeployIn(currentCID)
	mqHere := mr2mq[currentRoom]
	delete(mr2mq, currentRoom)
	if len(mqHere) > 0 {
		out = append(out, s.Inner.Allocate(consumerGroup, currentCID, mqHere, mr2c[currentRoom])...)
	}
	// 2. Rooms with no alive consumer: every consumer shares their queues.
	for _, room := range rooms {
		if room == currentRoom {
			continue
		}
		if _, alive := mr2c[room]; !alive {
			out = append(out, s.Inner.Allocate(consumerGroup, currentCID, mr2mq[room], cidAll)...)
		}
	}
	return out
}
