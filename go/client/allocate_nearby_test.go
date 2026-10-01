// AllocateMachineRoomNearby tests (Java
// org.apache.rocketmq.client.consumer.rebalance.AllocateMachineRoomNearBy).
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

type testResolver struct {
	brokerRooms map[string]string
	clientRooms map[string]string
}

func (r testResolver) BrokerDeployIn(mq common.MessageQueue) string {
	return r.brokerRooms[mq.BrokerName]
}

func (r testResolver) ConsumerDeployIn(clientID string) string { return r.clientRooms[clientID] }

func nearbyMQs() []common.MessageQueue {
	return []common.MessageQueue{
		{Topic: "T", BrokerName: "room-a-broker", QueueID: 0},
		{Topic: "T", BrokerName: "room-a-broker", QueueID: 1},
		{Topic: "T", BrokerName: "room-b-broker", QueueID: 0},
		{Topic: "T", BrokerName: "room-b-broker", QueueID: 1},
	}
}

// Queues of the current consumer's room are split among SAME-ROOM consumers
// only, and the orphan room's queues are shared by ALL consumers.
func TestAllocateMachineRoomNearbySplitsByRoomAndSharesOrphans(t *testing.T) {
	s, err := NewAllocateMachineRoomNearby(AllocateMessageQueueAveragely{}, testResolver{
		brokerRooms: map[string]string{"room-a-broker": "room-a", "room-b-broker": "room-b"},
		clientRooms: map[string]string{"c1": "room-a", "c2": "room-a"}, // nobody in room-b
	})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	got := s.Allocate("G", "c1", nearbyMQs(), []string{"c1", "c2"})
	if s.AllocateErr() != nil {
		t.Fatalf("unexpected allocate error: %v", s.AllocateErr())
	}
	// room-a has 2 queues / 2 consumers -> queue 0 for c1 (AVG inside the room);
	// room-b's 2 queues are shared by ALL consumers -> AVG over 2 queues / 2
	// consumers hands queue 0 there to c1 as well.
	if len(got) != 2 {
		t.Fatalf("got %d queues, want 2: %v", len(got), got)
	}
	for _, mq := range got {
		if mq.QueueID != 0 {
			t.Errorf("c1 got queue %d, want only the first queue of each room", mq.QueueID)
		}
	}
}

// The consumer's own room keeps its queues exclusive: a room-b consumer must
// not receive anything from room-a while room-a still has alive consumers.
func TestAllocateMachineRoomNearbyKeepsRoomsExclusive(t *testing.T) {
	s, _ := NewAllocateMachineRoomNearby(NewAllocateMessageQueueByConfig(nil), testResolver{
		brokerRooms: map[string]string{"room-a-broker": "room-a", "room-b-broker": "room-b"},
		clientRooms: map[string]string{"c1": "room-a", "c2": "room-b"},
	})
	got := s.Allocate("G", "c2", nearbyMQs(), []string{"c1", "c2"})
	if len(got) != 0 {
		t.Fatalf("CONFIG inner strategy with an empty configured list must yield nothing, got %v", got)
	}
}

// An empty machine room from the resolver is a hard failure (Java
// IllegalArgumentException): the error is reported through AllocateErrReporter
// so the rebalance round can keep the previous assignment.
func TestAllocateMachineRoomNearbyEmptyRoomIsReported(t *testing.T) {
	s, _ := NewAllocateMachineRoomNearby(AllocateMessageQueueAveragely{}, testResolver{
		brokerRooms: map[string]string{"room-a-broker": "room-a"},
		clientRooms: map[string]string{"c1": "room-a"},
	})
	got := s.Allocate("G", "c1", nearbyMQs(), []string{"c1"})
	if got != nil {
		t.Fatalf("a queue without a room must abort, got %v", got)
	}
	if err := s.AllocateErr(); err == nil || !strings.Contains(err.Error(), "machine room is null") {
		t.Fatalf("AllocateErr = %v, want a 'machine room is null' failure", err)
	}
}

// Same for a consumer with an empty room.
func TestAllocateMachineRoomNearbyEmptyConsumerRoomIsReported(t *testing.T) {
	s, _ := NewAllocateMachineRoomNearby(AllocateMessageQueueAveragely{}, testResolver{
		brokerRooms: map[string]string{"room-a-broker": "room-a"},
		clientRooms: map[string]string{"c1": "room-a", "c2": ""},
	})
	got := s.Allocate("G", "c1", []common.MessageQueue{{Topic: "T", BrokerName: "room-a-broker", QueueID: 0}},
		[]string{"c1", "c2"})
	if got != nil || s.AllocateErr() == nil {
		t.Fatalf("an empty consumer room must abort: got=%v err=%v", got, s.AllocateErr())
	}
}

func TestAllocateMachineRoomNearbyConstructorNullChecks(t *testing.T) {
	resolver := testResolver{}
	if _, err := NewAllocateMachineRoomNearby(nil, resolver); err == nil {
		t.Error("nil inner strategy must be rejected")
	}
	if _, err := NewAllocateMachineRoomNearby(AllocateMessageQueueAveragely{}, nil); err == nil {
		t.Error("nil resolver must be rejected")
	}
}

func TestAllocateMachineRoomNearbyNameCarriesInnerName(t *testing.T) {
	s, _ := NewAllocateMachineRoomNearby(AllocateMessageQueueAveragelyByCircle{}, testResolver{})
	if got := s.Name(); got != "MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE" {
		t.Fatalf("Name = %q, want MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE", got)
	}
}
