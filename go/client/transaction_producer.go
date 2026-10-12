package client

import (
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	DefaultCheckThreadPoolMinSize = 1
	DefaultCheckThreadPoolMaxSize = 1
	DefaultCheckRequestHoldMax    = 2000
)

type TransactionMQProducer struct {
	*DefaultMQProducer

	mu                  sync.Mutex
	transactionListener TransactionListener

	checkThreadPoolMinSize int
	checkThreadPoolMaxSize int
	checkRequestHoldMax    int

	executorService ExecutorService

	// ownedPool is the pool Start built from the tuning fields. It is the only
	// pool this producer is allowed to shut down.
	ownedPool *boundedPool
}

// Submit must NOT block indefinitely and should report rejection; the check-back
// path treats a rejection as "this check is dropped" and logs it.
type ExecutorService interface {
	Submit(task func()) error
	Shutdown()
}

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

func (t *TransactionMQProducer) Start() error {
	t.initTransactionEnv()
	return t.DefaultMQProducer.Start()
}

// Shutdown stops the check executor and then the producer.
func (t *TransactionMQProducer) Shutdown() {
	t.destroyTransactionEnv()
	t.DefaultMQProducer.Shutdown()
}

func (t *TransactionMQProducer) SendMessageInTransaction(msg *common.Message, arg any) (*TransactionSendResult, error) {
	listener := t.TransactionListener()
	if listener == nil {
		return nil, common.ClientError("TransactionListener is null")
	}
	return t.DefaultMQProducer.SendMessageInTransaction(msg, listener, arg)
}

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
