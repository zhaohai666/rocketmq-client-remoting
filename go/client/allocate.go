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
	Name() string
}

type AllocateErrReporter interface {
	AllocateErr() error
}

func strategyName(s AllocateMessageQueueStrategy) string {
	if s == nil {
		return "<nil>"
	}
	return s.Name()
}

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

type AllocateMessageQueueByConfig struct {
	MessageQueueList []common.MessageQueue
}

// NewAllocateMessageQueueByConfig builds the strategy.
func NewAllocateMessageQueueByConfig(mqs []common.MessageQueue) *AllocateMessageQueueByConfig {
	return &AllocateMessageQueueByConfig{MessageQueueList: append([]common.MessageQueue(nil), mqs...)}
}

func (*AllocateMessageQueueByConfig) Name() string { return "CONFIG" }

func (s *AllocateMessageQueueByConfig) Allocate(_, _ string, _ []common.MessageQueue, _ []string) []common.MessageQueue {
	return append([]common.MessageQueue(nil), s.MessageQueueList...)
}

// ------------------------------------------------- consistent hash ring

type hashFunction interface {
	Hash(key string) uint32
}

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
		v := virtualNode{key: virtualNodeKey(pNode, i+existing), physicalKey: pNode}
		key := r.hashFn.Hash(v.key)
		if _, ok := r.ring[key]; !ok {
			r.keys = insertU32(r.keys, key)
		}
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

func virtualNodeKey(pNode string, replicaIndex int) string {
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

func queueWireString(mq common.MessageQueue) string { return mq.WireString() }

// ------------------------------------------- CONSISTENT_HASH

type AllocateMessageQueueConsistentHash struct {
	VirtualNodeCnt     int
	CustomHashFunction hashFunction
}

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
		node, ok := router.routeNode(queueWireString(mq))
		if ok && node == currentCID {
			out = append(out, mq)
		}
	}
	return out
}

// ------------------------------------------------- MACHINE_ROOM

func splitWithoutTrailingEmpty(text, sep string) []string {
	parts := strings.Split(text, sep)
	if len(parts) == 1 {
		return parts
	}
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

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
	var candidates []common.MessageQueue
	for _, mq := range mqAll {
		parts := splitWithoutTrailingEmpty(mq.BrokerName, "@")
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

type MachineRoomResolver interface {
	BrokerDeployIn(mq common.MessageQueue) string
	ConsumerDeployIn(clientID string) string
}

//  1. the queues in THIS consumer's room are split among the consumers of that
//     room only (through the inner strategy);
//  2. queues in a room with NO alive consumer at all are shared among ALL
//     consumers (again through the inner strategy) — otherwise nobody would
//     consume them.
type AllocateMachineRoomNearby struct {
	Inner    AllocateMessageQueueStrategy
	Resolver MachineRoomResolver

	mu  sync.Mutex
	err error
}

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
	mr2mq := map[string][]common.MessageQueue{}
	rooms := make([]string, 0, len(mqAll))
	for _, mq := range mqAll {
		room := s.Resolver.BrokerDeployIn(mq)
		if room == "" {
			err := fmt.Errorf("machine room is null for mq %s", mq.WireString())
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
