// TransactionMQProducer — the transactional producer façade (Java
// org.apache.rocketmq.client.producer.TransactionMQProducer).
//
// The two-phase mechanics themselves live on DefaultMQProducer
// (SendMessageInTransaction + endTransaction + the CHECK_TRANSACTION_STATE
// responder). What this file adds is the Java wrapper's contribution:
//
//   - a stored TransactionListener, so the caller writes sendMessageInTransaction(msg, arg)
//     instead of passing the listener at every call site;
//   - a dedicated check-back executor (Java checkExecutor), so the broker's
//     CHECK_TRANSACTION_STATE flood is bounded instead of spawning one
//     goroutine per request. Java's default is min=1, max=1, queue=2000 —
//     a SINGLE worker, i.e. check-backs are serialised;
//   - the start/shutdown wiring for that pool (initTransactionEnv /
//     destroyTransactionEnv).
package client

import (
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Java TransactionMQProducer defaults (checkThreadPoolMinSize / MaxSize /
// checkRequestHoldMax).
const (
	DefaultCheckThreadPoolMinSize = 1
	DefaultCheckThreadPoolMaxSize = 1
	DefaultCheckRequestHoldMax    = 2000
)

// TransactionMQProducer mirrors Java TransactionMQProducer. The embedded
// pointer (rather than a value) is what makes the inherited send paths operate
// on THIS instance: Start/Shutdown here must configure the same producer the
// caller sends with.
type TransactionMQProducer struct {
	*DefaultMQProducer

	mu                  sync.Mutex
	transactionListener TransactionListener

	// checkThreadPoolMinSize/MaxSize/HoldMax are read once, when Start builds
	// the check executor. Java has the same "configure before start" rule (the
	// fields are only consulted by initTransactionEnv), and the three setters
	// are deprecated there in favour of SetExecutorService.
	checkThreadPoolMinSize int
	checkThreadPoolMaxSize int
	checkRequestHoldMax    int

	// executorService, when set, REPLACES the internally built pool (Java
	// `checkExecutor = producer.getExecutorService()`). The pool is then owned
	// by the caller and is NOT shut down by Shutdown — the same asymmetry Java
	// has, because destroyTransactionEnv only ever shuts down the pool it built.
	executorService ExecutorService

	// ownedPool is the pool Start built from the tuning fields. It is the only
	// pool this producer is allowed to shut down.
	ownedPool *boundedPool
}

// ExecutorService is the pluggable pool Java exposes via
// setExecutorService(ExecutorService). A caller that wants to reuse one pool
// across producers implements this over its own scheduler.
//
// Submit must NOT block indefinitely and should report rejection; the check-back
// path treats a rejection as "this check is dropped" and logs it.
type ExecutorService interface {
	Submit(task func()) error
	Shutdown()
}

// NewTransactionMQProducer builds a producer with the Java defaults.
func NewTransactionMQProducer(producerGroup string) (*TransactionMQProducer, error) {
	inner, err := NewDefaultMQProducer(producerGroup)
	if err != nil {
		return nil, err
	}
	t := &TransactionMQProducer{
		DefaultMQProducer:      inner,
		checkThreadPoolMinSize: DefaultCheckThreadPoolMinSize,
		checkThreadPoolMaxSize: DefaultCheckThreadPoolMaxSize,
		checkRequestHoldMax:    DefaultCheckRequestHoldMax,
	}
	// Java resolves the check-back listener through getCheckListener(), which
	// reads THIS wrapper's field — so the resolution is a property of being a
	// TransactionMQProducer, not of having started. Installed here (and left
	// installed) so a listener set before Start is still found, and one set
	// after Start is picked up by the closure.
	inner.setCheckListenerFn(t.TransactionListener)
	return t, nil
}

// MustNewTransactionMQProducer panics on an invalid group; convenient for
// examples.
func MustNewTransactionMQProducer(producerGroup string) *TransactionMQProducer {
	p, err := NewTransactionMQProducer(producerGroup)
	if err != nil {
		panic(err)
	}
	return p
}

// SetTransactionListener stores the listener used by both phases.
func (t *TransactionMQProducer) SetTransactionListener(listener TransactionListener) {
	t.mu.Lock()
	t.transactionListener = listener
	t.mu.Unlock()
}

// TransactionListener returns the stored listener (may be nil).
func (t *TransactionMQProducer) TransactionListener() TransactionListener {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.transactionListener
}

// SetExecutorService replaces the internally built check executor.
func (t *TransactionMQProducer) SetExecutorService(svc ExecutorService) {
	t.mu.Lock()
	t.executorService = svc
	t.mu.Unlock()
}

// ExecutorService returns the caller-supplied pool, if any.
func (t *TransactionMQProducer) ExecutorService() ExecutorService {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.executorService
}

func (t *TransactionMQProducer) SetCheckThreadPoolMinSize(n int) {
	t.mu.Lock()
	t.checkThreadPoolMinSize = n
	t.mu.Unlock()
}

func (t *TransactionMQProducer) CheckThreadPoolMinSize() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkThreadPoolMinSize
}

func (t *TransactionMQProducer) SetCheckThreadPoolMaxSize(n int) {
	t.mu.Lock()
	t.checkThreadPoolMaxSize = n
	t.mu.Unlock()
}

func (t *TransactionMQProducer) CheckThreadPoolMaxSize() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkThreadPoolMaxSize
}

func (t *TransactionMQProducer) SetCheckRequestHoldMax(n int) {
	t.mu.Lock()
	t.checkRequestHoldMax = n
	t.mu.Unlock()
}

func (t *TransactionMQProducer) CheckRequestHoldMax() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkRequestHoldMax
}

// Start builds the check executor BEFORE the producer starts, which is Java's
// order (TransactionMQProducer.start → initTransactionEnv → super.start): the
// broker can deliver a check-back the moment the producer registers, and a
// check arriving before the pool exists would have nowhere to run.
func (t *TransactionMQProducer) Start() error {
	t.initTransactionEnv()
	return t.DefaultMQProducer.Start()
}

// Shutdown stops the check executor and then the producer.
//
// Deliberately the REVERSE of Java's order (TransactionMQProducer.shutdown does
// super.shutdown() first, then destroyTransactionEnv). Java's order lets an
// in-flight check-back keep running after the producer's MQClientInstance is
// gone, so its endTransaction races with the teardown; draining the pool first
// makes Shutdown mean "nothing of mine is still running". Observable behaviour
// is otherwise identical — both halves complete before Shutdown returns.
func (t *TransactionMQProducer) Shutdown() {
	t.destroyTransactionEnv()
	t.DefaultMQProducer.Shutdown()
}

// SendMessageInTransaction uses the stored listener, mirroring Java
// TransactionMQProducer.sendMessageInTransaction(msg, arg).
//
// Java's error text is "TransactionListener is null" (capital L) — deliberately
// NOT the "tranExecutor is null" the 3-arg implementation path uses, so the two
// failures stay distinguishable.
func (t *TransactionMQProducer) SendMessageInTransaction(msg *common.Message, arg any) (*TransactionSendResult, error) {
	listener := t.TransactionListener()
	if listener == nil {
		return nil, common.ClientError("TransactionListener is null")
	}
	return t.DefaultMQProducer.SendMessageInTransaction(msg, listener, arg)
}

// initTransactionEnv mirrors Java DefaultMQProducerImpl.initTransactionEnv:208.
// A caller-supplied ExecutorService wins; otherwise the pool is built from the
// three (deprecated in Java) tuning fields.
//
// Re-entrant: Start called twice tears the first pool down instead of leaking
// it. Java leaks here (initTransactionEnv overwrites checkExecutor without
// shutting the old one down) — a leak we decline to reproduce, since it is
// invisible in a single-start process and only ever shows up as a stuck
// shutdown.
func (t *TransactionMQProducer) initTransactionEnv() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ownedPool != nil {
		t.ownedPool.Shutdown()
		t.ownedPool = nil
	}
	installed := t.executorService
	if installed == nil {
		installed = newBoundedPool("TransactionCheckExecutor",
			t.checkThreadPoolMaxSize, t.checkRequestHoldMax)
		if pool, ok := installed.(*boundedPool); ok {
			t.ownedPool = pool
		}
	}
	t.DefaultMQProducer.setCheckExecutor(installed)
}

// destroyTransactionEnv mirrors Java DefaultMQProducerImpl.destroyTransactionEnv:223.
// Only a pool this producer built is shut down; a caller-supplied
// ExecutorService belongs to the caller and is merely detached.
func (t *TransactionMQProducer) destroyTransactionEnv() {
	t.mu.Lock()
	owned := t.ownedPool
	t.ownedPool = nil
	t.mu.Unlock()

	t.DefaultMQProducer.setCheckExecutor(nil)
	if owned != nil {
		owned.Shutdown()
	}
}
