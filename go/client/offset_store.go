// Consumer offset stores (Java org.apache.rocketmq.client.consumer.store.*).
//
// RemoteBrokerOffsetStore is the CLUSTERING store: offsets live broker-side,
// the local table is a write-through cache. LocalFileOffsetStore is the
// BROADCASTING store: offsets live under
// $HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json in Java fastjson2's
// MessageQueue-as-object-key format, with offsets.json.bak as the previous
// generation.
package client

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ReadOffsetMode selects where ReadOffset looks.
type ReadOffsetMode int

// Java OffsetStore.ReadOffsetMode.
const (
	ReadFromMemory ReadOffsetMode = iota // MEMORY_FIRST semantics handled by caller
	ReadMemoryFirst
	ReadFromStore
)

// OffsetStore is the shared interface (Java OffsetStore).
type OffsetStore interface {
	// Load pulls persisted offsets in (remote: nothing to do).
	Load() error
	// UpdateOffset records a new offset; increaseOnly keeps the max.
	UpdateOffset(mq common.MessageQueue, offset int64, increaseOnly bool)
	// ReadOffset answers the committed offset. -1 means "unknown".
	ReadOffset(mq common.MessageQueue, mode ReadOffsetMode) (int64, error)
	// PersistAll flushes the named queues and drops table entries for queues
	// not named (Java's "remove unused mq").
	PersistAll(mqs []common.MessageQueue) error
	// Persist flushes one queue.
	Persist(mq common.MessageQueue) error
}

// ---------------------------------------------------------------- remote store

// RemoteBrokerOffsetStore backs CLUSTERING consumers.
type RemoteBrokerOffsetStore struct {
	instance *Instance
	group    string

	mu    sync.Mutex
	table map[common.MessageQueue]int64
}

// NewRemoteBrokerOffsetStore builds the store for one consumer group.
func NewRemoteBrokerOffsetStore(instance *Instance, group string) *RemoteBrokerOffsetStore {
	return &RemoteBrokerOffsetStore{
		instance: instance,
		group:    group,
		table:    map[common.MessageQueue]int64{},
	}
}

// Load is a no-op: remote offsets are fetched on demand.
func (s *RemoteBrokerOffsetStore) Load() error { return nil }

// UpdateOffset records the offset. A missing entry is created even for offset
// 0 (Java updateOffset's putIfAbsent — the second commit of a pair may carry
// the lower value and must not erase the first).
func (s *RemoteBrokerOffsetStore) UpdateOffset(mq common.MessageQueue, offset int64, increaseOnly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.table[mq]; ok {
		if increaseOnly && offset < old {
			return
		}
		s.table[mq] = offset
		return
	}
	s.table[mq] = offset
}

// ReadOffset answers from the table (memory modes) or the broker.
// -1 = unknown (no entry / broker says QUERY_NOT_FOUND).
func (s *RemoteBrokerOffsetStore) ReadOffset(mq common.MessageQueue, mode ReadOffsetMode) (int64, error) {
	if mode == ReadMemoryFirst || mode == ReadFromMemory {
		s.mu.Lock()
		offset, ok := s.table[mq]
		s.mu.Unlock()
		if ok {
			return offset, nil
		}
		if mode == ReadFromMemory {
			return -1, nil
		}
	}
	// READ_FROM_STORE (and MEMORY_FIRST on a table miss): ask the broker.
	offset, found, err := s.instance.QueryConsumerOffset(s.group, mq, DefaultMQClientAPITimeoutMillis, "", false)
	if err != nil {
		return -1, err
	}
	if !found {
		return -1, nil
	}
	s.UpdateOffset(mq, offset, false)
	return offset, nil
}

// PersistAll flushes the given queues to their brokers and removes table
// entries for queues not in the list (Java's "remove unused mq" — rebalanced-
// away queues must not re-appear on the next load). Broadcasting is the
// caller's concern; this store only ever runs for clustering consumers.
func (s *RemoteBrokerOffsetStore) PersistAll(mqs []common.MessageQueue) error {
	if len(mqs) == 0 {
		return nil
	}
	var firstErr error
	for _, mq := range mqs {
		s.mu.Lock()
		offset, ok := s.table[mq]
		s.mu.Unlock()
		if !ok {
			continue
		}
		if offset < 0 {
			continue
		}
		if err := s.updateConsumeOffsetToBroker(mq, offset); err != nil && firstErr == nil {
			firstErr = err
		}
	} // Drop entries the caller did not name — Java's removal condition is
	// literally `!mqs.contains(mq)`, so a named queue keeps its table slot
	// even when its offset was negative and never committed.
	requested := make(map[common.MessageQueue]struct{}, len(mqs))
	for _, mq := range mqs {
		requested[mq] = struct{}{}
	}
	s.mu.Lock()
	for mq := range s.table {
		if _, ok := requested[mq]; !ok {
			delete(s.table, mq)
		}
	}
	s.mu.Unlock()
	return firstErr
}

// Persist flushes one queue.
func (s *RemoteBrokerOffsetStore) Persist(mq common.MessageQueue) error {
	s.mu.Lock()
	offset, ok := s.table[mq]
	s.mu.Unlock()
	if !ok || offset < 0 {
		return nil
	}
	return s.updateConsumeOffsetToBroker(mq, offset)
}

// Table snapshots the cached table (checks and tests).
func (s *RemoteBrokerOffsetStore) Table() map[common.MessageQueue]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[common.MessageQueue]int64, len(s.table))
	for k, v := range s.table {
		out[k] = v
	}
	return out
}

// updateConsumeOffsetToBroker picks the address Java-style: master from the
// publish table first, then "any addr for this broker in a cached route"
// (findBrokerAddrByTopic fallback inside Java persistAll).
func (s *RemoteBrokerOffsetStore) updateConsumeOffsetToBroker(mq common.MessageQueue, offset int64) error {
	addr, ok := s.instance.FindBrokerAddressInPublish(mq.BrokerName)
	if !ok {
		if route := s.instance.GetTopicRouteData(mq.Topic); route != nil {
			addr, ok = FindBrokerAddrInRoute(route, mq.BrokerName)
		}
	}
	if !ok {
		return common.ClientError("The broker[" + mq.BrokerName + "] not exist")
	}
	return s.instance.UpdateConsumerOffset(s.group, mq, offset, DefaultMQClientAPITimeoutMillis, addr)
}

// ---------------------------------------------------------------- local store

// LocalFileOffsetStore backs BROADCASTING consumers with a JSON file:
// ~/.rocketmq_offsets/<clientId>/<group>/offsets.json.
type LocalFileOffsetStore struct {
	clientID string
	group    string

	mu    sync.Mutex
	table map[common.MessageQueue]int64

	// loadDir/read/write are swappable for tests.
	storePath string
}

// NewLocalFileOffsetStore builds the store under the default path
// (~/.rocketmq_offsets). An env override rocketmq.client.localOffsetStoreDir
// takes the place of the base directory, matching Java.
func NewLocalFileOffsetStore(clientID, group string) *LocalFileOffsetStore {
	base := os.Getenv("rocketmq.client.localOffsetStoreDir")
	if base == "" {
		base = filepath.Join(common.UserHome(), ".rocketmq_offsets")
	}
	return &LocalFileOffsetStore{
		clientID:  clientID,
		group:     group,
		table:     map[common.MessageQueue]int64{},
		storePath: filepath.Join(base, clientID, group, "offsets.json"),
	}
}

// Load reads offsets.json, falling back to offsets.json.bak when the main
// file is missing; both missing is an empty table, not an error (Java same).
func (s *LocalFileOffsetStore) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.storePath)
	if err != nil {
		data, err = os.ReadFile(s.storePath + ".bak")
		if err != nil {
			return nil
		}
	}
	table, err := decodeLocalOffsetTable(data)
	if err != nil {
		// A corrupt file must not brick the consumer; Java logs and keeps an
		// empty table.
		common.LogWarnf("load local offset store %s failed: %v", s.storePath, err)
		s.table = map[common.MessageQueue]int64{}
		return nil
	}
	s.table = table
	return nil
}

// UpdateOffset records the offset (increaseOnly keeps the max).
func (s *LocalFileOffsetStore) UpdateOffset(mq common.MessageQueue, offset int64, increaseOnly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.table[mq]; ok {
		if increaseOnly && offset < old {
			return
		}
		s.table[mq] = offset
		return
	}
	s.table[mq] = offset
}

// ReadOffset answers from memory only; -1 when the queue has no entry.
func (s *LocalFileOffsetStore) ReadOffset(mq common.MessageQueue, mode ReadOffsetMode) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset, ok := s.table[mq]
	if !ok {
		return -1, nil
	}
	return offset, nil
}

// PersistAll writes the whole table (all entries ARE this consumer's queues).
func (s *LocalFileOffsetStore) PersistAll(mqs []common.MessageQueue) error {
	return s.persistFile()
}

// Persist writes the whole table too (Java keeps one file per group).
func (s *LocalFileOffsetStore) Persist(mq common.MessageQueue) error {
	return s.persistFile()
}

func (s *LocalFileOffsetStore) persistFile() error {
	s.mu.Lock()
	table := make(map[common.MessageQueue]int64, len(s.table))
	for k, v := range s.table {
		table[k] = v
	}
	path := s.storePath
	s.mu.Unlock()

	body := encodeLocalOffsetTable(table)
	// Java MixAll.string2File: the FIRST write creates the file, every later
	// write rolls the previous content to offsets.json.bak before replacing.
	if _, err := os.Stat(path); err == nil {
		prev, err := os.ReadFile(path)
		if err == nil {
			if err := os.WriteFile(path+".bak", prev, 0o644); err != nil {
				return err
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

// StorePath exposes the backing file path (diagnostics).
func (s *LocalFileOffsetStore) StorePath() string { return s.storePath }

// encodeLocalOffsetTable writes Java fastjson2's shape:
//
//	{"offsetTable":{"{\"brokerName\":\"b\",\"queueId\":1,\"topic\":\"T\"}":9}}
//
// the outer keys being compact MessageQueue JSON text (Java fastjson2 writes
// object keys alphabetically: brokerName, queueId, topic).
func encodeLocalOffsetTable(table map[common.MessageQueue]int64) []byte {
	entries := make(map[string]any, len(table))
	for mq, offset := range table {
		entries[remoting.MessageQueueKeyJSON(mq)] = offset
	}
	return remoting.EncodeJSON(map[string]any{"offsetTable": entries})
}

// decodeLocalOffsetTable parses Java's compact and pretty variants. Legacy
// flat "topic+broker+queueId" string keys cannot round-trip through a
// MessageQueue-keyed table and are skipped (the file is rewritten in the Java
// shape on the next persist).
func decodeLocalOffsetTable(data []byte) (map[common.MessageQueue]int64, error) {
	out := map[common.MessageQueue]int64{}
	value, err := remoting.DecodeJSON(data)
	if err != nil {
		return out, err
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return out, common.DecodeError("local offset file: expected object")
	}
	raw, ok := obj["offsetTable"].(map[string]any)
	if !ok {
		if raw == nil {
			return out, nil
		}
		return out, common.DecodeError("local offset file: offsetTable is not an object")
	}
	for k, v := range raw {
		inner, ok := remoting.DecodeMapKey(k)
		if !ok {
			continue
		}
		m, ok := inner.(map[string]any)
		if !ok {
			continue
		}
		mq := common.NewMessageQueue(
			jsonString(m, "topic", ""),
			jsonString(m, "brokerName", ""),
			jsonI32Field(m, "queueId", 0),
		)
		offset, ok := jsonI64Value(v)
		if !ok {
			continue
		}
		out[mq] = offset
	}
	return out, nil
}

func jsonI64Value(v any) (int64, bool) {
	switch t := v.(type) {
	case remoting.JSONNumber:
		n, err := t.Int64()
		return n, err == nil
	case string:
		var n int64
		_, err := fmt.Sscanf(t, "%d", &n)
		return n, err == nil
	default:
		return 0, false
	}
}
