// Offset store tests: the remote store's broker round trips against the mock
// cluster (putIfAbsent, QUERY_NOT_FOUND, PersistAll's unused-queue sweep) and
// the local file store's Java-format compatibility (golden shape, .bak roll,
// .bak fallback, legacy keys, corruption).
package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- remote store

func TestRemoteOffsetStoreFlows(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		switch req.Code {
		case remoting.ReqQueryConsumerOffset:
			var header remoting.QueryConsumerOffsetRequestHeader
			header.FromExtFields(req.ExtFields())
			if header.QueueID != nil && *header.QueueID == 5 {
				return remoting.CreateResponseCommand(remoting.RespQueryNotFound, "")
			}
			resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
			resp.AddExtField("offset", "12")
			return resp
		case remoting.ReqUpdateConsumerOffset:
			s.mu.Lock()
			s.reqs = append(s.reqs, req)
			s.mu.Unlock()
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	store := NewRemoteBrokerOffsetStore(inst, "CG")
	mq0 := common.NewMessageQueue("Tt", "b1", 0)
	mq1 := common.NewMessageQueue("Tt", "b1", 1)

	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	// ReadFromMemory on an empty table is -1 without touching the broker.
	if offset, err := store.ReadOffset(mq0, ReadFromMemory); err != nil || offset != -1 {
		t.Fatalf("empty memory read = %d err=%v, want -1 nil", offset, err)
	}
	if n := len(broker.requests(remoting.ReqQueryConsumerOffset)); n != 0 {
		t.Fatalf("ReadFromMemory must not hit the broker, got %d queries", n)
	}

	// putIfAbsent: the second commit of a pair may carry the lower value.
	store.UpdateOffset(mq0, 5, false)
	store.UpdateOffset(mq0, 3, true)
	if offset := store.Table()[mq0]; offset != 5 {
		t.Fatalf("increaseOnly must keep the max, got %d", offset)
	}
	store.UpdateOffset(mq0, 8, true)
	if offset := store.Table()[mq0]; offset != 8 {
		t.Fatalf("higher offset must overwrite, got %d", offset)
	}

	// QUERY_NOT_FOUND(22) reads as -1, no error. mq0 is in the table now, so
	// the not-found probe must use a queue the consumer never touched.
	mqNF := common.NewMessageQueue("Tt", "b1", 5)
	if offset, err := store.ReadOffset(mqNF, ReadMemoryFirst); err != nil || offset != -1 {
		t.Fatalf("not-found read = %d err=%v, want -1 nil", offset, err)
	}
	// A found offset lands in the table.
	if offset, err := store.ReadOffset(mq1, ReadFromStore); err != nil || offset != 12 {
		t.Fatalf("broker read = %d err=%v, want 12 nil", offset, err)
	}
	if offset := store.Table()[mq1]; offset != 12 {
		t.Fatalf("broker read must cache, table has %d", offset)
	}
}

func TestRemoteOffsetStorePersistAllDropsUnused(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		switch req.Code {
		case remoting.ReqQueryConsumerOffset:
			resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
			resp.AddExtField("offset", "0")
			return resp
		case remoting.ReqUpdateConsumerOffset:
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	store := NewRemoteBrokerOffsetStore(inst, "CG")
	mq0 := common.NewMessageQueue("Tt", "b1", 0)
	mq1 := common.NewMessageQueue("Tt", "b1", 1)
	mq2 := common.NewMessageQueue("Tt", "b1", 2)
	store.UpdateOffset(mq0, 5, false)
	store.UpdateOffset(mq1, 4, false)
	store.UpdateOffset(mq2, -1, false) // negative offsets are never committed

	if err := store.PersistAll([]common.MessageQueue{mq0, mq2}); err != nil {
		t.Fatal(err)
	}
	updates := broker.requests(remoting.ReqUpdateConsumerOffset)
	if len(updates) != 1 {
		t.Fatalf("PersistAll must commit exactly the used non-negative queue, got %d updates", len(updates))
	}
	var header remoting.UpdateConsumerOffsetRequestHeader
	header.FromExtFields(updates[0].ExtFields())
	if header.Topic == nil || *header.Topic != "Tt" || header.QueueID == nil || *header.QueueID != 0 ||
		header.CommitOffset == nil || *header.CommitOffset != 5 {
		t.Errorf("commit payload mismatch: %+v", header)
	}
	// Rebalanced-away queues leave the table; a NAMED queue keeps its slot
	// even when its offset was negative (Java's `!mqs.contains(mq)` sweep).
	table := store.Table()
	if _, ok := table[mq1]; ok {
		t.Errorf("unused queue must be dropped from the table")
	}
	if offset := table[mq2]; offset != -1 {
		t.Errorf("named negative-offset queue must keep its slot, got %d", offset)
	}
	if offset := table[mq0]; offset != 5 {
		t.Errorf("kept queue lost its offset: %d", offset)
	}

	// Persist(nil) is a no-op, not an error.
	if err := store.PersistAll(nil); err != nil {
		t.Errorf("PersistAll(nil) must be a no-op, got %v", err)
	}
}

// ---------------------------------------------------------------- local store

func newLocalStore(t *testing.T, clientID, group string) *LocalFileOffsetStore {
	t.Helper()
	t.Setenv("rocketmq.client.localOffsetStoreDir", t.TempDir())
	return NewLocalFileOffsetStore(clientID, group)
}

func TestLocalOffsetStoreGoldenLoad(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	path := store.StorePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	golden := `{"offsetTable":{"{\"brokerName\":\"b1\",\"queueId\":1,\"topic\":\"Tt\"}":9}}`
	if err := os.WriteFile(path, []byte(golden), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	mq := common.NewMessageQueue("Tt", "b1", 1)
	if offset, err := store.ReadOffset(mq, ReadFromMemory); err != nil || offset != 9 {
		t.Fatalf("golden load = %d err=%v, want 9 nil", offset, err)
	}
}

func TestLocalOffsetStorePersistRollsBak(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	mq := common.NewMessageQueue("Tt", "b1", 0)
	path := store.StorePath()

	store.UpdateOffset(mq, 5, false)
	if err := store.Persist(mq); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("first persist must create the file: %v", err)
	}
	if _, err := os.Stat(path + ".bak"); !os.IsNotExist(err) {
		t.Fatalf("first persist must NOT create a .bak, stat err=%v", err)
	}

	store.UpdateOffset(mq, 6, false)
	if err := store.Persist(mq); err != nil {
		t.Fatal(err)
	}
	prev, err := os.ReadFile(path + ".bak")
	if err != nil {
		t.Fatalf("second persist must roll the previous content to .bak: %v", err)
	}
	prevTable, err := decodeLocalOffsetTable(prev)
	if err != nil || prevTable[mq] != 5 {
		t.Errorf(".bak must hold the previous generation, got %s (decode %v)", prev, err)
	}
	cur, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	curTable, err := decodeLocalOffsetTable(cur)
	if err != nil || curTable[mq] != 6 {
		t.Errorf("main file must hold the new generation, got %s (decode %v)", cur, err)
	}
}

func TestLocalOffsetStoreBakFallback(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	path := store.StorePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	golden := `{"offsetTable":{"{\"brokerName\":\"b1\",\"queueId\":2,\"topic\":\"Tt\"}":7}}`
	if err := os.WriteFile(path+".bak", []byte(golden), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	mq := common.NewMessageQueue("Tt", "b1", 2)
	if offset, err := store.ReadOffset(mq, ReadFromMemory); err != nil || offset != 7 {
		t.Fatalf("bak fallback = %d err=%v, want 7 nil", offset, err)
	}
}

func TestLocalOffsetStoreSkipsLegacyKeys(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	path := store.StorePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	mixed := `{"offsetTable":{"Tt+b1+2":9,"{\"brokerName\":\"b1\",\"queueId\":1,\"topic\":\"Tt\"}":4}}`
	if err := os.WriteFile(path, []byte(mixed), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	// The legacy flat key targets a different queueId: if it survived, both
	// queues would answer.
	if offset, err := store.ReadOffset(common.NewMessageQueue("Tt", "b1", 1), ReadFromMemory); err != nil || offset != 4 {
		t.Fatalf("surviving entry = %d err=%v, want 4", offset, err)
	}
	if offset, err := store.ReadOffset(common.NewMessageQueue("Tt", "b1", 2), ReadFromMemory); err != nil || offset != -1 {
		t.Fatalf("legacy flat key must be skipped, got %d err=%v", offset, err)
	}
}

func TestLocalOffsetStoreCorruptFileEmptiesTable(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	path := store.StorePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A corrupt file must not brick the consumer.
	if err := store.Load(); err != nil {
		t.Fatalf("corrupt load must be swallowed, got %v", err)
	}
	mq := common.NewMessageQueue("Tt", "b1", 0)
	if offset, err := store.ReadOffset(mq, ReadFromMemory); err != nil || offset != -1 {
		t.Fatalf("after corrupt load table must be empty, got %d err=%v", offset, err)
	}
}

func TestLocalOffsetStoreBothFilesMissingIsFine(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	if err := store.Load(); err != nil {
		t.Fatalf("missing files must load as empty, got %v", err)
	}
}

func TestLocalOffsetStoreIncreaseOnly(t *testing.T) {
	store := newLocalStore(t, "cid", "CG")
	mq := common.NewMessageQueue("Tt", "b1", 3)
	store.UpdateOffset(mq, 5, false)
	store.UpdateOffset(mq, 2, true)
	if offset, _ := store.ReadOffset(mq, ReadFromMemory); offset != 5 {
		t.Fatalf("increaseOnly must keep the max, got %d", offset)
	}
	// Writing without increaseOnly still lowers (Java updateOffset without the flag).
	store.UpdateOffset(mq, 1, false)
	if offset, _ := store.ReadOffset(mq, ReadFromMemory); offset != 1 {
		t.Fatalf("plain update must overwrite, got %d", offset)
	}
}
