// TransactionMQProducer tests: the wrapper's own surface (stored listener,
// check-back executor lifecycle) rather than the two-phase wire mechanics,
// which producer_test.go already covers through DefaultMQProducer.
package client

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- fixtures

func startTransactionFixture(t *testing.T, topic, group string) (*TransactionMQProducer, *mockServer) {
	t.Helper()
	handler := &sendBrokerOnReq{offset: 5}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewTransactionMQProducer(group)
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("transaction producer start: %v", err)
	}
	t.Cleanup(p.Shutdown)
	return p, broker
}

// concurrencyProbeListener records how many CheckLocalTransaction calls were in
// flight at the same time, which is the observable consequence of the
// single-worker check executor.
type concurrencyProbeListener struct {
	mu          sync.Mutex
	calls       int
	inflight    int
	maxInflight int
	hold        time.Duration
}

func (l *concurrencyProbeListener) ExecuteLocalTransaction(*common.Message, any) LocalTransactionState {
	return Unknow
}

func (l *concurrencyProbeListener) CheckLocalTransaction(*common.MessageExt) LocalTransactionState {
	l.mu.Lock()
	l.calls++
	l.inflight++
	if l.inflight > l.maxInflight {
		l.maxInflight = l.inflight
	}
	l.mu.Unlock()

	if l.hold > 0 {
		time.Sleep(l.hold)
	}

	l.mu.Lock()
	l.inflight--
	l.mu.Unlock()
	return RollbackMessage
}

func (l *concurrencyProbeListener) snapshot() (calls, maxInflight int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls, l.maxInflight
}

// countingExecutorService is a caller-supplied pool that records whether the
// producer wrongly shut it down.
type countingExecutorService struct {
	mu       sync.Mutex
	submits  int
	shutdown bool
}

func (s *countingExecutorService) Submit(task func()) error {
	s.mu.Lock()
	s.submits++
	s.mu.Unlock()
	task()
	return nil
}

func (s *countingExecutorService) Shutdown() {
	s.mu.Lock()
	s.shutdown = true
	s.mu.Unlock()
}

func (s *countingExecutorService) state() (submits int, shutdown bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submits, s.shutdown
}

// openBrokerConn forces the producer to dial the broker, which the mock needs
// before it can push a server-initiated request back (pushOneway fails with
// "no client connection to push on" otherwise).
func openBrokerConn(t *testing.T, p *DefaultMQProducer, topic string) {
	t.Helper()
	if _, err := p.Send(common.NewMessage(topic, []byte("prime"))); err != nil {
		t.Fatalf("priming send: %v", err)
	}
}

// ---------------------------------------------------------------- tests

func TestTransactionProducerUsesStoredListener(t *testing.T) {
	const topic = "TxProdStoredTopic"
	p, broker := startTransactionFixture(t, topic, "GID_tx_stored")

	listener := &recordingTransactionListener{executeState: CommitMessage, checkState: Unknow}
	if p.TransactionListener() != nil {
		t.Fatal("no listener should be set before the setter runs")
	}
	p.SetTransactionListener(listener)
	if p.TransactionListener() != listener {
		t.Fatal("the setter must store the listener")
	}

	result, err := p.SendMessageInTransaction(common.NewMessage(topic, []byte("tx")), "arg")
	if err != nil {
		t.Fatalf("transaction send: %v", err)
	}
	if result.LocalTransactionState != CommitMessage {
		t.Errorf("state = %s", result.LocalTransactionState)
	}
	if executeCalls, _, _ := listener.snapshot(); executeCalls != 1 {
		t.Errorf("executeLocalTransaction called %d times, want 1", executeCalls)
	}
	waitFor(t, "END_TRANSACTION", func() bool {
		return len(broker.requests(remoting.ReqEndTransaction)) >= 1
	})

	// The stored listener is also the one the check-back uses, so the
	// producer answers even without a per-call listener.
	ext := common.ExtFromMessage(common.NewMessage(topic, []byte("tx")))
	ext.PutProperty(common.PropertyProducerGroup, "GID_tx_stored")
	body, err := common.EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cmd := remoting.CreateRequestCommand(remoting.ReqCheckTransactionState,
		&remoting.CheckTransactionStateRequestHeader{Topic: strPtr(topic), Bname: strPtr("b1")})
	cmd.SetBody(body)
	cmd.MarkOnewayRPC()
	if err := broker.pushOneway(cmd); err != nil {
		t.Fatalf("push check-back: %v", err)
	}
	waitFor(t, "the stored listener to be used for the check-back", func() bool {
		_, checkCalls, _ := listener.snapshot()
		return checkCalls == 1
	})
}

func TestTransactionProducerRequiresListener(t *testing.T) {
	const topic = "TxProdNoListenerTopic"
	p, broker := startTransactionFixture(t, topic, "GID_tx_no_listener")

	_, err := p.SendMessageInTransaction(common.NewMessage(topic, []byte("tx")), nil)
	if err == nil {
		t.Fatal("a transaction send without a listener must fail")
	}
	// Java's wrapper uses this exact text, distinct from the "tranExecutor is
	// null" the inner implementation raises.
	if !strings.Contains(err.Error(), "TransactionListener is null") {
		t.Errorf("error = %v, want Java's wrapper wording", err)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("nothing may be sent, but the broker saw %d requests", got)
	}
}

func TestTransactionProducerStartCreatesCheckExecutor(t *testing.T) {
	const topic = "TxProdExecTopic"
	p, _ := startTransactionFixture(t, topic, "GID_tx_exec")

	svc := p.CheckExecutor()
	if svc == nil {
		t.Fatal("Start must install the check executor")
	}
	pool, ok := svc.(*boundedPool)
	if !ok {
		t.Fatalf("expected the built-in pool, got %T", svc)
	}
	// Java's defaults: min=1, max=1, queue=2000.
	if got := pool.QueueCapacity(); got != DefaultCheckRequestHoldMax {
		t.Errorf("queue capacity = %d, want %d", got, DefaultCheckRequestHoldMax)
	}
	if p.CheckThreadPoolMaxSize() != 1 || p.CheckThreadPoolMinSize() != 1 {
		t.Errorf("pool sizes = %d/%d, want 1/1",
			p.CheckThreadPoolMinSize(), p.CheckThreadPoolMaxSize())
	}

	// Shutdown reduces the executor to nil and rejects further submissions.
	p.Shutdown()
	if p.CheckExecutor() != nil {
		t.Error("Shutdown must detach the check executor")
	}
	if err := pool.Submit(func() {}); err == nil {
		t.Error("the pool must reject after Shutdown")
	}
}

func TestTransactionProducerHonoursExecutorServiceOverride(t *testing.T) {
	const topic = "TxProdExecOverrideTopic"
	p, broker := startTransactionFixture(t, topic, "GID_tx_exec_override")

	// Replacing the pool after Start has no effect until the next Start — the
	// same "configure before start" rule Java has. Start again with the
	// override to prove it wins.
	custom := &countingExecutorService{}
	p.SetExecutorService(custom)
	if err := p.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if p.CheckExecutor() != ExecutorService(custom) {
		t.Fatalf("the caller-supplied service must replace the built pool, got %T", p.CheckExecutor())
	}

	listener := &recordingTransactionListener{executeState: Unknow, checkState: RollbackMessage}
	p.SetTransactionListener(listener)
	openBrokerConn(t, p.DefaultMQProducer, topic)

	ext := common.ExtFromMessage(common.NewMessage(topic, []byte("tx")))
	ext.PutProperty(common.PropertyProducerGroup, "GID_tx_exec_override")
	body, err := common.EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cmd := remoting.CreateRequestCommand(remoting.ReqCheckTransactionState,
		&remoting.CheckTransactionStateRequestHeader{Topic: strPtr(topic), Bname: strPtr("b1")})
	cmd.SetBody(body)
	cmd.MarkOnewayRPC()
	if err := broker.pushOneway(cmd); err != nil {
		t.Fatalf("push check-back: %v", err)
	}
	waitFor(t, "the check-back routed through the caller's pool", func() bool {
		submits, _ := custom.state()
		return submits >= 1
	})

	p.Shutdown()
	// Java's destroyTransactionEnv only shuts down the pool it built; a
	// caller-supplied ExecutorService is merely detached.
	if _, shutdown := custom.state(); shutdown {
		t.Error("Shutdown must not close a caller-supplied ExecutorService")
	}
	if p.CheckExecutor() != nil {
		t.Error("the override must still be detached on Shutdown")
	}
}

func TestTransactionProducerCheckExecutorSerialisesCheckBacks(t *testing.T) {
	const topic = "TxProdSerialTopic"
	p, broker := startTransactionFixture(t, topic, "GID_tx_serial")

	probe := &concurrencyProbeListener{hold: 40 * time.Millisecond}
	p.SetTransactionListener(probe)
	openBrokerConn(t, p.DefaultMQProducer, topic)

	const checkBacks = 3
	for i := 0; i < checkBacks; i++ {
		ext := common.ExtFromMessage(common.NewMessage(topic, []byte("tx")))
		ext.PutProperty(common.PropertyProducerGroup, "GID_tx_serial")
		body, err := common.EncodeMessageExt(ext, false)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		cmd := remoting.CreateRequestCommand(remoting.ReqCheckTransactionState,
			&remoting.CheckTransactionStateRequestHeader{Topic: strPtr(topic), Bname: strPtr("b1")})
		cmd.SetBody(body)
		cmd.MarkOnewayRPC()
		if err := broker.pushOneway(cmd); err != nil {
			t.Fatalf("push check-back %d: %v", i, err)
		}
	}

	// Wait for the END_TRANSACTION each check-back emits, not just for the
	// listener to have been entered: the listener counter goes up before the
	// verdict is reported, and the test would then tear the producer down while
	// a check is still in flight.
	waitFor(t, "all check-backs to run to completion", func() bool {
		return len(broker.requests(remoting.ReqEndTransaction)) >= checkBacks
	})
	adds, maxInflight := probe.snapshot()
	if adds != checkBacks {
		t.Errorf("checkLocalTransaction ran %d times, want %d", adds, checkBacks)
	}
	// Default pool: ONE worker. If the checks overlapped, the pool (or the
	// routing into it) is wrong.
	if maxInflight != 1 {
		t.Errorf("max concurrent checkLocalTransaction = %d, want 1", maxInflight)
	}
}

func TestTransactionProducerShutdownIsIdempotent(t *testing.T) {
	const topic = "TxProdShutdownTwiceTopic"
	p, _ := startTransactionFixture(t, topic, "GID_tx_shutdown_twice")
	p.Shutdown()
	p.Shutdown()
	if p.CheckExecutor() != nil {
		t.Error("the executor must stay detached")
	}
}
