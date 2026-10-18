// DefaultMQProducer — the client-side send API over the classic remoting
// protocol.
//
// Scope of this file: lifecycle, the six send paths (sync, to-queue, batch,
// oneway, selector, transaction) plus the retry/fault-avoidance loop, the send
// header assembly, the SEND response mapping and the broker's transaction
// check-back. Send latency fault tolerance lives in fault_strategy.go, its
// optional reachability detector in fault_detector.go.
package client

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Producer defaults.
const (
	DefaultSendMsgTimeout             = int64(3000)
	DefaultCompressMsgBodyOverHowmuch = 1024 * 4
	DefaultCompressLevel              = 5
	DefaultRetryTimesWhenSendFailed   = 2
	DefaultMaxMessageSize             = 1024 * 1024 * 4
	DefaultHeartbeatIntervalMillis    = int64(30_000)
	defaultPollNameServerInterval     = int64(30_000)
)

// Pinned-send topic guards — the sync and async paths reject the same mistake
// with two different fixed texts; callers may match on them.
const (
	pinnedTopicMismatchSync  = "message's topic not equal mq's topic"
	pinnedTopicMismatchAsync = "Topic of the message does not match its target message queue"
)

// delayProperties are the delay/timer property keys a transactional message
// may not carry — four keys: DELAY plus the three 5.x TIMER_* aliases, which
// are real protocol constants now, so the list covers them all.
var delayProperties = []string{
	common.PropertyDelayTimeLevel, // "DELAY"
	common.PropertyTimerDelayMs,   // "TIMER_DELAY_MS"
	common.PropertyTimerDelaySec,  // "TIMER_DELAY_SEC"
	common.PropertyTimerDeliverMs, // "TIMER_DELIVER_MS"
}

// delayClassProperties are the keys that make a SendMessageContext classify as
// DelayMsg. Deliberately a SUPERSET of
// delayProperties: the literal "__STARTDELIVERTIME" is a delay marker for the
// trace but not something ensureNotDelayedForTransactional rejects, and
// foldering the two lists into one would silently change both behaviours.
var delayClassProperties = []string{
	common.PropertyStartDeliverTime, // "__STARTDELIVERTIME"
	common.PropertyDelayTimeLevel,   // "DELAY"
	common.PropertyTimerDeliverMs,   // "TIMER_DELIVER_MS"
	common.PropertyTimerDelaySec,    // "TIMER_DELAY_SEC"
	common.PropertyTimerDelayMs,     // "TIMER_DELAY_MS"
}

// ---------------------------------------------------------------- selectors

// MessageQueueSelector picks the queue a message is sent to.
type MessageQueueSelector interface {
	Select(mqs []common.MessageQueue, msg *common.Message, arg any) (common.MessageQueue, error)
}

// SelectMessageQueueByHash picks by arg's hash (String.hashCode semantics:
// polynomial-31 over the string's UTF-16 code units).
//
// The hash must be reproducible across processes — that is the point of a
// sharding key — so it is computed explicitly instead of relying on any
// per-process randomised builtin hash.
type SelectMessageQueueByHash struct{}

func (SelectMessageQueueByHash) Select(mqs []common.MessageQueue, _ *common.Message, arg any) (common.MessageQueue, error) {
	if len(mqs) == 0 {
		return common.MessageQueue{}, common.ClientError("no message queue")
	}
	hash := common.StringHash31(fmt.Sprintf("%v", arg))
	// Math.abs(Integer.MIN_VALUE) is still negative in 32-bit int arithmetic;
	// collapse it to 0 explicitly.
	var idx int
	if hash < 0 {
		if hash == -1<<31 {
			idx = 0
		} else {
			idx = int(-hash) % len(mqs)
		}
	} else {
		idx = int(hash) % len(mqs)
	}
	return mqs[idx], nil
}

// SelectMessageQueueByRandom picks a random queue.
type SelectMessageQueueByRandom struct{}

func (SelectMessageQueueByRandom) Select(mqs []common.MessageQueue, _ *common.Message, _ any) (common.MessageQueue, error) {
	if len(mqs) == 0 {
		return common.MessageQueue{}, common.ClientError("no message queue")
	}
	return mqs[rand.Intn(len(mqs))], nil
}

// SelectMessageQueueByMachineRoom picks the first queue whose brokerName starts
// with arg; with no match it falls back to the first queue.
type SelectMessageQueueByMachineRoom struct{}

func (SelectMessageQueueByMachineRoom) Select(mqs []common.MessageQueue, _ *common.Message, arg any) (common.MessageQueue, error) {
	if len(mqs) == 0 {
		return common.MessageQueue{}, common.ClientError("no message queue")
	}
	room := fmt.Sprintf("%v", arg)
	for _, mq := range mqs {
		if strings.HasPrefix(mq.BrokerName, room) {
			return mq, nil
		}
	}
	return mqs[0], nil
}

// ---------------------------------------------------------------- callbacks

// SendCallback receives the outcome of an async send. OnSuccess and OnException
// are mutually exclusive and each fires exactly once.
type SendCallback interface {
	OnSuccess(result *SendResult)
	OnException(err error)
}

// SendCallbackFunc adapts two functions to SendCallback.
type SendCallbackFunc struct {
	SuccessFn func(*SendResult)
	ExceptFn  func(error)
}

func (c SendCallbackFunc) OnSuccess(result *SendResult) {
	if c.SuccessFn != nil {
		c.SuccessFn(result)
	}
}

func (c SendCallbackFunc) OnException(err error) {
	if c.ExceptFn != nil {
		c.ExceptFn(err)
	}
}

// ---------------------------------------------------------------- send hooks

// RegisterSendMessageHook appends a send hook, run before/after every send.
func (p *DefaultMQProducer) RegisterSendMessageHook(hook SendMessageHook) {
	if hook == nil {
		return
	}
	p.mu.Lock()
	p.sendMessageHooks = append(p.sendMessageHooks, hook)
	p.mu.Unlock()
}

// HasSendMessageHook reports whether the before/after chain must run. It is the
// fast path the send kernel checks before building a context.
func (p *DefaultMQProducer) HasSendMessageHook() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sendMessageHooks) > 0
}

// RegisterCheckForbiddenHook appends a pre-send interceptor.
func (p *DefaultMQProducer) RegisterCheckForbiddenHook(hook CheckForbiddenHook) {
	if hook == nil {
		return
	}
	p.mu.Lock()
	p.checkForbiddenHooks = append(p.checkForbiddenHooks, hook)
	p.mu.Unlock()
}

// HasCheckForbiddenHook reports whether the interceptor chain must run.
func (p *DefaultMQProducer) HasCheckForbiddenHook() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.checkForbiddenHooks) > 0
}

// RegisterEndTransactionHook appends an end-transaction hook.
func (p *DefaultMQProducer) RegisterEndTransactionHook(hook EndTransactionHook) {
	if hook == nil {
		return
	}
	p.mu.Lock()
	p.endTransactionHooks = append(p.endTransactionHooks, hook)
	p.mu.Unlock()
}

// HasEndTransactionHook reports whether the end-transaction chain must run.
func (p *DefaultMQProducer) HasEndTransactionHook() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.endTransactionHooks) > 0
}

// hasSendInterceptors reports whether either hook family forces
// the send kernel down the "build a context" path.
func (p *DefaultMQProducer) hasSendInterceptors() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sendMessageHooks) > 0 || len(p.checkForbiddenHooks) > 0
}

// hookSnapshot copies the hook lists so a hook that registers another hook
// while running cannot make the iteration grow, and so the send path does not
// hold p.mu across user code.
func (p *DefaultMQProducer) hookSnapshot() ([]SendMessageHook, []CheckForbiddenHook, []EndTransactionHook) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]SendMessageHook(nil), p.sendMessageHooks...),
		append([]CheckForbiddenHook(nil), p.checkForbiddenHooks...),
		append([]EndTransactionHook(nil), p.endTransactionHooks...)
}

// setCheckExecutor installs (or clears, with nil) the transaction check-back
// pool. TransactionMQProducer owns the lifecycle; nothing else should call it.
func (p *DefaultMQProducer) setCheckExecutor(svc ExecutorService) {
	p.mu.Lock()
	p.checkExecutor = svc
	p.mu.Unlock()
}

// setCheckListenerFn installs the resolver for the check-back listener.
// TransactionMQProducer points it at its own TransactionListener
// field so a wrapper's listener answers check-backs even before any send.
func (p *DefaultMQProducer) setCheckListenerFn(fn func() TransactionListener) {
	p.mu.Lock()
	p.checkListenerFn = fn
	p.mu.Unlock()
}

// CheckExecutor returns the current check-back pool, or nil.
func (p *DefaultMQProducer) CheckExecutor() ExecutorService {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkExecutor
}

// buildSendContext assembles the SendMessageContext for one attempt.
//
// The msgType derivation is deliberately a two-step override, not an
// else-if: a transactional message that also carries a delay property ends up
// as DelayMsg, because the delay classification is a separate `if` that
// overrides the transaction one.
func (p *DefaultMQProducer) buildSendContext(msg *common.Message, mq common.MessageQueue,
	brokerAddr string, mode CommunicationMode) *SendMessageContext {

	ctx := &SendMessageContext{
		Producer:          p,
		ProducerGroup:     p.producerGroup,
		CommunicationMode: mode,
		BornHost:          common.LocalIP(),
		BrokerAddr:        brokerAddr,
		Message:           msg,
		MQ:                mq,
		MsgType:           common.NormalMsg,
		Namespace:         p.namespace,
	}
	if v, ok := msg.GetProperty(common.PropertyTransactionPrepared); ok && v == "true" {
		ctx.MsgType = common.TransMsgHalf
	}
	for _, key := range delayClassProperties {
		if _, ok := msg.GetProperty(key); ok {
			ctx.MsgType = common.DelayMsg
			break
		}
	}
	return ctx
}

// checkForbidden runs the pre-send interceptors. Only the fields above are
// populated — and despite the field existing on CheckForbiddenContext, `arg`
// is never set, not even on the
// selector path (it is consumed by Select itself, not passed down).
// Arg therefore stays nil here too; the field is kept so an implementation can
// read it if a caller ever wires it up.
func (p *DefaultMQProducer) checkForbidden(hooks []CheckForbiddenHook,
	msg *common.Message, mq common.MessageQueue, brokerAddr string,
	mode CommunicationMode) error {

	if len(hooks) == 0 {
		return nil
	}
	ctx := &CheckForbiddenContext{
		NameServerAddr:    p.NamesrvAddr(),
		ProducerGroup:     p.producerGroup,
		CommunicationMode: mode,
		Message:           msg,
		MQ:                mq,
		BrokerAddr:        brokerAddr,
		UnitMode:          p.unitMode,
	}
	return executeCheckForbiddenHooks(hooks, ctx)
}

// doExecuteEndTransactionHook fires the end-transaction hooks:
// the hook fires whether the endTransaction came from the caller or from the
// broker's check-back, and `fromTransactionCheck` is the only thing that tells
// the two apart.
func (p *DefaultMQProducer) doExecuteEndTransactionHook(hooks []EndTransactionHook,
	msg *common.Message, msgID, brokerAddr string, state LocalTransactionState,
	fromTransactionCheck bool) {

	if len(hooks) == 0 {
		return
	}
	ctx := &EndTransactionContext{
		ProducerGroup:        p.producerGroup,
		Message:              msg,
		BrokerAddr:           brokerAddr,
		MsgID:                msgID,
		TransactionState:     state,
		FromTransactionCheck: fromTransactionCheck,
		Namespace:            p.namespace,
	}
	if msg != nil {
		ctx.TransactionID = msg.TransactionID
	}
	executeEndTransactionHooks(hooks, ctx)
}

// ---------------------------------------------------------------- transaction

// TransactionListener drives the two phases of a transactional message.
//
// ExecuteLocalTransaction runs once, right after the half message was accepted
// (only on SEND_OK). CheckLocalTransaction runs whenever the broker checks the
// half message back — potentially in an entirely different process lifetime.
// Returning Unknow (or a nil error path that leaves state alone) keeps the
// message invisible; the broker will check again after transactionCheckMax.
type TransactionListener interface {
	ExecuteLocalTransaction(msg *common.Message, arg any) LocalTransactionState
	CheckLocalTransaction(msg *common.MessageExt) LocalTransactionState
}

// ---------------------------------------------------------------- producer

// DefaultMQProducer is the producer facade: lifecycle plus all send paths.
type DefaultMQProducer struct {
	mu sync.Mutex

	producerGroup string
	namespace     string
	instanceName  string
	clientID      string

	unitName                string
	unitMode                bool
	enableStreamRequestType bool

	createTopicKey          string
	defaultTopicQueueNums   int32
	sendMsgTimeout          int64
	requestTimeout          int64
	compressMsgBodyOverHow  int
	compressLevel           int
	compressType            int32
	retryTimesWhenSendFail  int
	retryAnotherBrokerOK    bool
	sendMsgMaxTimeoutPerReq int64
	retryResponseCodes      map[int32]struct{}
	maxMessageSize          int

	rpcHook            remoting.RPCHook
	namespaceV2        string
	topics             []string
	nameServerAddrs    []string
	tlsEnable          *bool
	socksProxy         *remoting.SocksProxyConfig
	pollNameServerIntv int64

	// sendMessageWithVIPChannel
	// (default false): when set, the SEND rpcs go to the VIP port (broker port-2).
	sendMessageWithVIPChannel bool

	sendLatencyFaultEnable bool
	faultStrategy          *mqFaultStrategy

	// Client-side trace. The dispatcher is built and
	// started by Start and flushed by Shutdown.
	enableTrace      bool
	traceMsgBatchNum int
	traceTopic       string
	traceDispatcher  *AsyncTraceDispatcher
	// enableTraceContext is the W3C traceparent passthrough switch; it is
	// independent of enableTrace and defaults from ROCKETMQ_TRACE_CONTEXT_ENABLE.
	enableTraceContext bool

	mqClient *Instance
	started  bool

	transactionListener TransactionListener
	// checkListenerFn supplies the check-back listener. TransactionMQProducer points it
	// at its own field (a wrapper's listener must be honoured even before any
	// send); a plain producer leaves it nil and falls back to whatever
	// SendMessageInTransaction stored.
	checkListenerFn func() TransactionListener

	// Send-side hook lists. Registration is not documented as thread-safe, but the send paths read
	// them from arbitrary goroutines, so they live behind p.mu like the rest of
	// the mutable state.
	sendMessageHooks    []SendMessageHook
	checkForbiddenHooks []CheckForbiddenHook
	endTransactionHooks []EndTransactionHook

	// checkExecutor is the pool the broker's CHECK_TRANSACTION_STATE check-backs
	// run on. Nil on a plain
	// DefaultMQProducer — and on a TransactionMQProducer that has not started —
	// in which case the check-back falls back to a bare goroutine.
	checkExecutor ExecutorService

	// ---- async send ----
	//
	// These scalars are read from the sender and callback goroutines while a
	// user may be retuning them — the back-pressure knobs are RUNTIME knobs —
	// so they are atomics
	// rather than p.mu-guarded fields.
	enableBackpressureForAsyncMode atomic.Bool
	backPressureForAsyncSendNum    atomic.Int64
	backPressureForAsyncSendSize   atomic.Int64
	retryTimesWhenSendAsyncFailed  atomic.Int64
	asyncSenderQueueCapacity       atomic.Int64
	callbackExecutorThreads        atomic.Int64

	// The two back-pressure semaphores are built once by initAsyncConfig and
	// never replaced: resizing shifts the total permits IN PLACE (setTotalPermits
	// semantics), which is observably identical to swapping the whole semaphore
	// object — so no read-write lock is needed here.
	semaphoreAsyncSendNum  *fairSemaphore
	semaphoreAsyncSendSize *fairSemaphore

	// The two async executors are created by Start and torn down by Shutdown, so
	// a send can race with a shutdown; atomic.Pointer keeps that read lock-free.
	asyncSenderPool atomic.Pointer[boundedPool]
	callbackPool    atomic.Pointer[boundedPool]

	heartbeatIntervalMillis int64
	heartbeatRunning        atomic.Bool
	// detectorRunning drives the fault-avoidance reachability probe goroutine
	// (fault_detector.go); started by Start, stopped by Shutdown.
	detectorRunning atomic.Bool

	// ---- auto-batch send (ProduceAccumulator) ----
	// autoBatch is a pre-Start config flag; autoBatchReady + accumulatorPtr are
	// published by Start once the accumulator is live and read lock-free on every
	// send (the hot path must not take p.mu).
	autoBatch       bool
	batchMaxDelayMs int64
	batchMaxBytes   int64
	autoBatchReady  atomic.Bool
	accumulatorPtr  atomic.Pointer[ProduceAccumulator]
}

// NewDefaultMQProducer validates the group and applies the SDK defaults.
func NewDefaultMQProducer(producerGroup string) (*DefaultMQProducer, error) {
	if common.IsBlank(producerGroup) {
		return nil, common.ClientError("producerGroup is empty")
	}
	p := &DefaultMQProducer{
		producerGroup:           producerGroup,
		instanceName:            common.DefaultInstanceName,
		createTopicKey:          common.DefaultTopic,
		defaultTopicQueueNums:   common.DefaultTopicQueueNums,
		sendMsgTimeout:          DefaultSendMsgTimeout,
		requestTimeout:          DefaultRequestTimeoutMillis,
		compressMsgBodyOverHow:  DefaultCompressMsgBodyOverHowmuch,
		compressLevel:           DefaultCompressLevel,
		compressType:            common.ZlibType,
		retryTimesWhenSendFail:  DefaultRetryTimesWhenSendFailed,
		sendMsgMaxTimeoutPerReq: -1,
		maxMessageSize:          DefaultMaxMessageSize,
		pollNameServerIntv:      defaultPollNameServerInterval,
		heartbeatIntervalMillis: DefaultHeartbeatIntervalMillis,
		traceMsgBatchNum:        defaultTraceMsgBatchNum,
		enableTraceContext:      TraceContextEnabledFromEnv(),
		retryResponseCodes: map[int32]struct{}{
			// RetryResponseCodes: only these codes
			// are worth trying on another broker. Anything else (e.g.
			// MESSAGE_ILLEGAL) would fail identically everywhere, so it must be
			// surfaced as-is instead of burning the retry budget.
			remoting.RespSystemError:         {},
			remoting.RespSystemBusy:          {},
			remoting.RespServiceNotAvailable: {},
			remoting.RespNoPermission:        {},
			remoting.RespTopicNotExist:       {},
			remoting.RespNoBuyerId:           {},
			remoting.RespNotInCurrentUnit:    {},
			remoting.RespGoAway:              {},
		},
	}
	p.faultStrategy = newMQFaultStrategy(false)
	// Back-pressure semaphores and the async knobs: the semaphores are built
	// right here, at construction; the two async EXECUTORS are built
	// later, in Start.
	p.initAsyncConfig()
	return p, nil
}

// MustNewDefaultMQProducer panics on an invalid group; convenient for examples.
func MustNewDefaultMQProducer(producerGroup string) *DefaultMQProducer {
	p, err := NewDefaultMQProducer(producerGroup)
	if err != nil {
		panic(err)
	}
	return p
}

// ---------------- configuration ----------------

func (p *DefaultMQProducer) ProducerGroup() string { return p.producerGroup }
func (p *DefaultMQProducer) ClientID() string      { return p.clientID }
func (p *DefaultMQProducer) NamesrvAddr() string   { return strings.Join(p.nameServerAddrs, ";") }

func (p *DefaultMQProducer) SetProducerGroup(group string) { p.producerGroup = group }
func (p *DefaultMQProducer) SetNamespace(ns string)        { p.namespace = ns }
func (p *DefaultMQProducer) SetInstanceName(name string)   { p.instanceName = name }
func (p *DefaultMQProducer) SetUnitName(unit string)       { p.unitName = unit }
func (p *DefaultMQProducer) SetUnitMode(mode bool)         { p.unitMode = mode }
func (p *DefaultMQProducer) SetEnableStreamRequestType(e bool) {
	p.enableStreamRequestType = e
}
func (p *DefaultMQProducer) SetRpcHook(hook remoting.RPCHook) { p.rpcHook = hook }

// SetNamespaceV2 mirrors ClientConfig#setNamespaceV2: the SERVER-side namespace
// (sent as the `ns`/`nsd` extFields by NamespaceRpcHook), as opposed to
// SetNamespace, which mangles topic names client-side.
func (p *DefaultMQProducer) SetNamespaceV2(ns string) { p.namespaceV2 = ns }

func (p *DefaultMQProducer) SetTopics(topics []string) { p.topics = append([]string(nil), topics...) }
func (p *DefaultMQProducer) SetTLSEnable(enable bool)  { p.tlsEnable = &enable }

func (p *DefaultMQProducer) SetSocksProxy(proxy *remoting.SocksProxyConfig) {
	p.socksProxy = proxy
}

func (p *DefaultMQProducer) SetPollNameServerIntervalMillis(v int64) {
	p.pollNameServerIntv = v
}
func (p *DefaultMQProducer) SetHeartbeatIntervalMillis(v int64) { p.heartbeatIntervalMillis = v }
func (p *DefaultMQProducer) SetSendMsgTimeout(v int64)          { p.sendMsgTimeout = v }
func (p *DefaultMQProducer) SetRequestTimeout(v int64)          { p.requestTimeout = v }
func (p *DefaultMQProducer) SetMaxMessageSize(v int)            { p.maxMessageSize = v }
func (p *DefaultMQProducer) SetRetryTimesWhenSendFailed(n int)  { p.retryTimesWhenSendFail = n }
func (p *DefaultMQProducer) SetSendMsgMaxTimeoutPerRequest(v int64) {
	p.sendMsgMaxTimeoutPerReq = v
}

// SetRetryAnotherBrokerWhenNotStoreOK: on a non-SEND_OK status the send is
// retried on another broker instead of being returned as-is.
func (p *DefaultMQProducer) SetRetryAnotherBrokerWhenNotStoreOK(v bool) {
	p.retryAnotherBrokerOK = v
}

func (p *DefaultMQProducer) SetCompressMsgBodyOverHowmuch(v int) { p.compressMsgBodyOverHow = v }
func (p *DefaultMQProducer) SetCompressLevel(level int)          { p.compressLevel = level }
func (p *DefaultMQProducer) SetCompressType(t int32)             { p.compressType = t }
func (p *DefaultMQProducer) SetCreateTopicKey(key string)        { p.createTopicKey = key }
func (p *DefaultMQProducer) SetDefaultTopicQueueNums(n int32)    { p.defaultTopicQueueNums = n }

// SetSendMessageWithVIPChannel:
// when set, the send rpcs target the broker's VIP port (listen port - 2,
// common.BrokerVIPChannel). Off by default — a cluster without the VIP listener
// would make every send time out on a dead port.
func (p *DefaultMQProducer) SetSendMessageWithVIPChannel(enable bool) {
	p.sendMessageWithVIPChannel = enable
}

// IsSendMessageWithVIPChannel reports the current VIP-channel switch.
func (p *DefaultMQProducer) IsSendMessageWithVIPChannel() bool { return p.sendMessageWithVIPChannel }

// sendAddr applies the VIP-channel rewrite to a resolved publish address;
// every send invoke goes through it.
func (p *DefaultMQProducer) sendAddr(addr string) string {
	if p.sendMessageWithVIPChannel {
		return common.BrokerVIPChannel(true, addr)
	}
	return addr
}

// SetSendLatencyFaultEnable toggles the fault strategy. It can be flipped at
// runtime, so it is not a start-time-only setting.
func (p *DefaultMQProducer) SetSendLatencyFaultEnable(enable bool) {
	p.faultStrategy.SetSendLatencyFaultEnable(enable)
}

func (p *DefaultMQProducer) IsSendLatencyFaultEnable() bool {
	return p.faultStrategy.SendLatencyFaultEnable()
}

// ---------------------------------------------------------------- trace

// SetEnableTrace turns on message tracing: Start then
// brings up an AsyncTraceDispatcher in PRODUCE mode whose SendMessageTraceHook
// records every send. Off by default.
func (p *DefaultMQProducer) SetEnableTrace(enable bool) { p.enableTrace = enable }

// IsEnableTrace reports the tracing switch.
func (p *DefaultMQProducer) IsEnableTrace() bool { return p.enableTrace }

// SetTraceMsgBatchNum sets how many trace records are batched before a flush;
// the dispatcher caps it at 20.
func (p *DefaultMQProducer) SetTraceMsgBatchNum(n int) { p.traceMsgBatchNum = n }

// SetTraceTopic overrides the trace topic (default common.TraceTopic).
func (p *DefaultMQProducer) SetTraceTopic(topic string) { p.traceTopic = topic }

// TraceDispatcher exposes the running dispatcher, or nil when tracing is off.
func (p *DefaultMQProducer) TraceDispatcher() *AsyncTraceDispatcher { return p.traceDispatcher }

// SetEnableTraceContext turns the W3C traceparent passthrough on or off. The
// constructor seeds it from ROCKETMQ_TRACE_CONTEXT_ENABLE.
func (p *DefaultMQProducer) SetEnableTraceContext(enable bool) { p.enableTraceContext = enable }

// IsEnableTraceContext reports the current traceparent switch.
func (p *DefaultMQProducer) IsEnableTraceContext() bool { return p.enableTraceContext }

// AddRetryResponseCode extends the retryable broker-code set.
func (p *DefaultMQProducer) AddRetryResponseCode(code int32) {
	p.retryResponseCodes[code] = struct{}{}
}

func (p *DefaultMQProducer) IsRetryResponseCode(code int32) bool {
	_, ok := p.retryResponseCodes[code]
	return ok
}

// SetNameServerAddr replaces the nameserver list. The address list is a
// semicolon-separated string; empty entries are dropped.
func (p *DefaultMQProducer) SetNameServerAddr(addrs string) {
	p.nameServerAddrs = splitNamesrvAddr(addrs)
}

// SetNameServerAddresses sets the nameserver list directly.
func (p *DefaultMQProducer) SetNameServerAddresses(addrs []string) {
	p.nameServerAddrs = append([]string(nil), addrs...)
}

// splitNamesrvAddr parses the address-list form: split on ';' and trim.
func splitNamesrvAddr(addrs string) []string {
	var out []string
	for _, part := range strings.Split(addrs, ";") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// ---------------------------------------------------------------- lifecycle

// Start validates the group (after
// namespacing), builds the clientId, brings up the shared client instance, then
// registers the producer group and starts the heartbeat thread.
//
// The heartbeat is NOT cosmetic: the broker's transaction check-back looks the
// producer group up in its channel table, which is filled from these
// heartbeats. Without it COMMIT/ROLLBACK still work (the client pushes
// END_TRANSACTION itself) but an UNKNOW half message is never checked back.
func (p *DefaultMQProducer) Start() error {
	started, err := p.startLocked()
	if err != nil || !started {
		return err
	}
	// The trace dispatcher starts outside the lock, AFTER the producer core is
	// up: it creates and
	// starts an internal producer of its own (a network operation) and registers
	// its hooks through RegisterSendMessageHook, which takes p.mu.
	p.startTraceDispatcher()
	p.startDetector()
	return nil
}

// startLocked is Start's body; it reports whether THIS call brought the producer
// up (a second Start is a no-op).
func (p *DefaultMQProducer) startLocked() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return false, nil
	}
	// The broker registers the NAMESPACED (PREFIXED) producer group name —
	// checkConfig runs after this rewrite.
	if p.namespace != "" {
		p.producerGroup = common.WrapNamespace(p.namespace, p.producerGroup)
	}
	if err := common.CheckGroup(p.producerGroup); err != nil {
		return false, err
	}
	if p.producerGroup == common.DefaultProducerGroup {
		return false, common.ClientError(fmt.Sprintf(
			"producerGroup can not equal %s, please specify another one.", common.DefaultProducerGroup))
	}
	if len(p.nameServerAddrs) == 0 {
		// The protocol surfaces a missing nameserver on the first send (as
		// 10004); reporting it
		// here is more predictable, but must use the SAME code so callers do
		// not have to handle two different values for one failure.
		return false, common.ClientErrorCode(common.NoNameServerException, "name server address is not set")
	}
	// Start first converts the instance name to the PID, then builds the
	// clientId. The instance name is written back in place, so a second
	// Start() reuses the same clientId instead of minting a new one.
	p.instanceName = common.ChangeInstanceNameToPID(p.instanceName)
	if p.clientID == "" {
		p.clientID = common.ClientIDFor(p.instanceName, p.unitName, p.enableStreamRequestType)
	}
	cfg := NewClientInstanceConfig()
	if p.tlsEnable != nil {
		cfg.TLSEnable = *p.tlsEnable
	}
	cfg.SocksProxy = p.socksProxy
	cfg.PollNameServerIntervalMillis = p.pollNameServerIntv
	inst := CreateOrGetInstance(p.clientID, p.nameServerAddrs, cfg)
	// Namespace + stream + user(ACL) + zone, composed in a fixed order and only
	// once per instance — the signature has to cover exactly these fields.
	inst.EnsureRPCHooks(p.namespaceV2, p.enableStreamRequestType, p.rpcHook)
	if err := inst.Start(); err != nil {
		return false, err
	}
	// A dynamic nameserver (TopAddressing) may have resolved addresses during
	// instance start; copy them back so the producer reports them.
	if len(p.nameServerAddrs) == 0 && len(inst.NameServerAddrs()) > 0 {
		p.nameServerAddrs = append([]string(nil), inst.NameServerAddrs()...)
	}
	// Broker-initiated requests: transaction check-back (39). Matched against
	// the PGROUP property; other groups' checks are dropped.
	//
	// NOTE: the transport keeps ONE processor per request code, so two producers
	// sharing a clientId (same instanceName) would have the second one clobber
	// the first. (A single processor that dispatches by group would avoid this,
	// but this port registers per producer.)
	// Producers
	// started without SetInstanceName get a unique clientId each, so the
	// default configuration is unaffected.
	inst.Remoting().RegisterProcessor(remoting.ReqCheckTransactionState, p.handleCheckTransactionState)
	// Register with the client instance — the factory's shutdown guard needs it.
	inst.RegisterProducer(p.producerGroup)
	for _, topic := range p.topics {
		inst.RegisterTopicInUse(topic)
	}
	// Build the async executors. Done here rather than in the constructor so that
	// a producer which never starts never owns two live goroutine pools; the
	// mirror image is destroyAsyncExecutors in Shutdown.
	p.initAsyncExecutors()
	if p.autoBatch {
		acc := NewProduceAccumulator(p.clientID)
		if p.batchMaxDelayMs > 0 {
			_ = acc.BatchMaxDelayMs(p.batchMaxDelayMs)
		}
		if p.batchMaxBytes > 0 {
			_ = acc.BatchMaxBytes(p.batchMaxBytes)
		}
		acc.Start()
		p.accumulatorPtr.Store(acc)
		p.autoBatchReady.Store(true)
	}
	p.mqClient = inst
	p.started = true
	p.heartbeatRunning.Store(true)
	go p.heartbeatLoop()
	return true, nil
}

// startTraceDispatcher: with
// enableTrace on, build a PRODUCE dispatcher, register its send and
// end-transaction hooks, then start it. Failures only log — a broken trace
// stack must never take the producer down.
func (p *DefaultMQProducer) startTraceDispatcher() {
	p.mu.Lock()
	enable := p.enableTrace
	batchNum := p.traceMsgBatchNum
	topic := p.traceTopic
	group := p.producerGroup
	nameSrv := strings.Join(p.nameServerAddrs, ";")
	rpcHook := p.rpcHook
	dispatcher := p.traceDispatcher
	if enable && dispatcher == nil {
		dispatcher = NewAsyncTraceDispatcher(group, TraceDispatcherProduce, batchNum, topic, rpcHook)
		dispatcher.SetHostProducer(p)
		p.traceDispatcher = dispatcher
	}
	p.mu.Unlock()

	if enable && group != "" {
		p.RegisterSendMessageHook(NewSendMessageTraceHook(dispatcher))
		p.RegisterEndTransactionHook(NewEndTransactionTraceHook(dispatcher))
	}
	if dispatcher != nil {
		if err := dispatcher.Start(nameSrv, AccessChannelLocal); err != nil {
			common.LogWarnf("trace dispatcher start failed: %v", err)
		}
	}
}

// Shutdown stops the producer, unregisters the group and releases the client
// instance.
func (p *DefaultMQProducer) Shutdown() {
	// Drain the async executors FIRST, and deliberately without p.mu held:
	// draining waits for in-flight tasks, and those take p.mu themselves
	// (requireClient, hookSnapshot), so holding it here would deadlock against
	// them. When this returns, no async attempt of ours can still be running; a
	// submit that arrives afterwards is rejected with "producer already
	// shutdown".
	p.destroyAsyncExecutors()

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.heartbeatRunning.Store(false)
	p.stopDetector()
	// Stop the auto-batch guard before the transport goes away so no flush
	// races an unregistered client.
	if acc := p.accumulatorPtr.Load(); acc != nil {
		p.autoBatchReady.Store(false)
		acc.Shutdown()
	}
	if p.mqClient != nil {
		// Graceful unregister (UNREGISTER_CLIENT = 35) per broker BEFORE the
		// transport goes away: the broker's ProducerManager state is per broker,
		// and without this it only expires via the channel scan (~120s), during
		// which the broker still considers the group connected — and would pick
		// this dead channel for a transaction check-back.
		p.mqClient.UnregisterClientAllBrokers(p.producerGroup, "")
		// Order matters: unregisterProducer first, then shut the factory down.
		p.mqClient.UnregisterProducer(p.producerGroup)
		p.mqClient.Shutdown()
		p.mqClient.DetachFromRegistryIfLastTenant()
	}
	p.mqClient = nil
	p.started = false
	dispatcher := p.traceDispatcher
	p.traceDispatcher = nil
	p.mu.Unlock()

	// The trace dispatcher goes last, outside the lock: it must flush what is
	// still queued and wait for it, and it sends through its OWN internal
	// producer (unaffected by the host instance having just shut down) — so it
	// is torn down only after the host producer is fully shut down.
	if dispatcher != nil {
		dispatcher.Shutdown()
	}
}

// heartbeatLoop periodically advertises this producer group to every broker
// via a ProducerData heartbeat.
func (p *DefaultMQProducer) heartbeatLoop() {
	interval := p.heartbeatIntervalMillis
	if interval <= 0 {
		interval = DefaultHeartbeatIntervalMillis
	}
	for p.heartbeatRunning.Load() {
		if err := p.sendHeartbeatToAllBroker(); err != nil {
			common.LogDebugf("producer heartbeat failed: %v", err)
		}
		// Sleep in slices so Shutdown is prompt.
		deadline := time.Now().Add(time.Duration(interval) * time.Millisecond)
		for p.heartbeatRunning.Load() && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (p *DefaultMQProducer) sendHeartbeatToAllBroker() error {
	p.mu.Lock()
	inst := p.mqClient
	clientID := p.clientID
	group := p.producerGroup
	p.mu.Unlock()
	if inst == nil {
		return nil
	}
	addrs := inst.GetRouteOfAllBrokers()
	if len(addrs) == 0 {
		return nil
	}
	hb := remoting.NewHeartbeatData(clientID)
	hb.AddProducerData(remoting.NewProducerData(group))
	var firstErr error
	for _, addr := range addrs {
		if err := inst.SendHeartbeat(addr, hb, 5000); err != nil {
			common.LogWarnf("producer heartbeat to %s failed: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// ---------------------------------------------------------------- helpers

func (p *DefaultMQProducer) requireClient() (*Instance, error) {
	if !p.started || p.mqClient == nil {
		return nil, common.ClientError("producer not started, call start() first")
	}
	return p.mqClient, nil
}

// checkMessage runs the local message validation — purely local,
// runs before any network I/O.
func (p *DefaultMQProducer) checkMessage(msg *common.Message) error {
	return common.CheckMessage(msg, p.maxMessageSize)
}

// withNamespace prefixes the configured namespace onto a topic name.
func (p *DefaultMQProducer) withNamespace(topic string) string {
	if p.namespace == "" {
		return topic
	}
	return common.WrapNamespace(p.namespace, topic)
}

// validateNameServerSetting separates a missing nameserver from a missing
// route. It only runs on the
// "no route" branch, to separate two very different failures:
//   - no nameserver address at all (including "address server configured but
//     returned nothing") -> 10004;
//   - addresses present, this topic simply has no route -> let the original
//     error stand.
//
// Without this step a misconfigured address server surfaces as "No route info
// of this topic", which reads as "topic does not exist" and sends people down
// the wrong path entirely.
func (p *DefaultMQProducer) validateNameServerSetting() error {
	if p.mqClient == nil || len(p.mqClient.NameServerAddrs()) == 0 {
		return common.ClientErrorCode(common.NoNameServerException, "No name server address, please set it.")
	}
	return nil
}

// topicPublishInfo
// fetches the REAL route first, and only when that genuinely fails (a brand new
// topic not yet registered on the nameserver) fall back to the default topic
// (TBW102) to synthesize publish info. Consumers deliberately do NOT do this
// fallback.
func (p *DefaultMQProducer) topicPublishInfo(topic string) (*TopicPublishInfo, error) {
	inst, err := p.requireClient()
	if err != nil {
		return nil, err
	}
	inst.RegisterTopicInUse(topic)
	info, err := inst.GetTopicPublishInfo(topic, false)
	if err == nil {
		return info, nil
	}
	info, err = inst.GetTopicPublishInfo(topic, true)
	if err == nil {
		return info, nil
	}
	if nsErr := p.validateNameServerSetting(); nsErr != nil {
		return nil, nsErr
	}
	return nil, err
}

// checkPinnedTopic guards a send to a pinned queue. It compares the two names
// AFTER namespacing each side (the message topic and the queue's topic are both
// wrapped), because wrapNamespace is idempotent.
//
// Without this guard a message whose topic does not match its target queue is
// still accepted: the broker writes to the queue named in the request, the
// SendResult looks perfectly fine, and the message lands in ANOTHER topic's
// partition where nobody will ever consume it.
func (p *DefaultMQProducer) checkPinnedTopic(topic string, mq common.MessageQueue, message string) error {
	if p.withNamespace(mq.Topic) != topic {
		return common.ClientError(message)
	}
	return nil
}

// responseCodeOf extracts a broker/client business code from an error.
func responseCodeOf(err error) (int32, bool) {
	if e, ok := err.(*common.Error); ok {
		return e.ResponseCode()
	}
	return 0, false
}

// ---------------------------------------------------------------- compression

// tryToCompressMessage compresses msg.Body IN PLACE when it is at/over the
// threshold and returns the sysFlag to send. 0 means "not compressed".
//
// Behaviour, fixed by the send kernel contract:
//   - batch messages are NEVER compressed;
//   - the threshold is compressMsgBodyOverHowmuch (default 4096), compared with
//     >=;
//   - a compression failure degrades to "not compressed" with a log line, it
//     does not fail the send;
//   - the compressed size is NOT compared against the original.
//
// Callers must invoke this ONCE, outside the retry loop, and restore the body
// afterwards (every call site keeps prevBody and defers the restore).
//
// The reference send kernel compresses once per ATTEMPT, capturing the original
// body and restoring it in a `finally`, so every retry re-compresses the
// ORIGINAL body. Compressing once here and restoring at the end yields
// byte-identical payloads and sysFlag, and skips the redundant recompression of
// a body that is already compressed.
func (p *DefaultMQProducer) tryToCompressMessage(msg *common.Message, isBatch bool) int32 {
	if isBatch {
		return 0
	}
	body := msg.Body
	if len(body) == 0 || len(body) < p.compressMsgBodyOverHow {
		return 0
	}
	compressed, err := common.Compress(body, p.compressType, int32(p.compressLevel))
	if err != nil {
		common.LogWarnf("tryToCompressMessage failed, send uncompressed: %v", err)
		return 0
	}
	if len(compressed) == 0 {
		return 0
	}
	msg.SetBody(compressed)
	return common.SetCompressionType(common.MessageSysFlagCompressed, p.compressType)
}

// ---------------------------------------------------------------- request

// buildSendRequest assembles the SEND request: the V2 compact header plus the
// body.
//
// Rebuilt per attempt on purpose (broker name and queue id change on retry),
// including the quirk that the %RETRY% header
// hoisting below only fires on the first attempt, because the source
// properties were cleared in place.
func (p *DefaultMQProducer) buildSendRequest(msg *common.Message, isBatch bool, mq common.MessageQueue, sysFlag int32) *remoting.RemotingCommand {
	// Non-batch messages get a client-side unique
	// id BEFORE the request is built. Batch messages already have per-message
	// ids from GenerateFromList. This id becomes SendResult.msgId and is what
	// the console uses to join the publish and consume traces.
	if !isBatch {
		common.SetUniqID(msg)
	}
	header := &remoting.SendMessageRequestHeaderV2{
		ProducerGroup:         strPtr(p.producerGroup),
		Topic:                 strPtr(msg.Topic),
		DefaultTopic:          strPtr(p.createTopicKey),
		DefaultTopicQueueNums: i32Ptr(p.defaultTopicQueueNums),
		QueueID:               i32Ptr(mq.QueueID),
		SysFlag:               i32Ptr(sysFlag),
		BornTimestamp:         i64Ptr(time.Now().UnixMilli()),
		Flag:                  i32Ptr(msg.Flag),
		Properties:            strPtr(common.MessageProperties2String(msg.Properties)),
		ReconsumeTimes:        i32Ptr(0),
		UnitMode:              boolPtr(p.unitMode),
		// maxReconsumeTimes is only sent for a %RETRY% topic carrying a
		// MAX_RECONSUME_TIMES property. Since client version V3_4_9 the broker
		// honours this field
		// unconditionally, so always
		// sending 0 makes a retry message land in %DLQ% on its very first
		// delivery (reconsumeTimes(0) >= 0).
		MaxReconsumeTimes: nil,
		Batch:             boolPtr(isBatch),
		// The V2 extFields key for the broker name is the single letter "n"
		// (compact wire header). Which broker this is
		// comes from the selected route queue.
		BrokerName: strPtr(mq.BrokerName),
	}
	// For `%RETRY%<group>` two retry properties
	// must be HOISTED into the request header, because the broker's
	// handleRetryAndDLQ reads
	// requestHeader.reconsumeTimes / maxReconsumeTimes rather than the message
	// properties. Missing the hoist is not an error but a SILENT misjudgement:
	// the broker falls back to the subscription group's retryMaxTimes (16) and
	// a message that should have gone to the DLQ after three tries keeps
	// circulating on the retry topic.
	//
	// The clear only affects this local message; the serialized properties
	// above already captured RECONSUME_TIME. Keep this ordering — do
	// not "tidy" it by moving the clear earlier.
	if strings.HasPrefix(msg.Topic, common.RetryGroupTopicPrefix) {
		if raw, ok := msg.GetProperty(common.PropertyReconsumeTime); ok {
			if v, err := parseInt32(raw); err == nil {
				header.ReconsumeTimes = i32Ptr(v)
			}
			msg.RemoveProperty(common.PropertyReconsumeTime)
		}
		if raw, ok := msg.GetProperty(common.PropertyMaxReconsumeTimes); ok {
			if v, err := parseInt32(raw); err == nil {
				header.MaxReconsumeTimes = i32Ptr(v)
			}
			msg.RemoveProperty(common.PropertyMaxReconsumeTimes)
		}
	}
	// Request-Reply: a MSG_TYPE == "reply" message goes out as
	// SEND_REPLY_MESSAGE_V2(325), not plain SEND_MESSAGE_V2(310) — the broker
	// only registers ReplyMessageProcessor for 324/325, and that processor is
	// what pushes the reply back to the requester by REPLY_TO_CLIENT.
	// Batch messages go out as SEND_BATCH_MESSAGE(320); the dispatch order is
	// reply first, then batch.
	var code int32
	switch {
	case isReplyMessage(msg):
		code = remoting.ReqSendReplyMessageV2
	case isBatch:
		code = remoting.ReqSendBatchMessage
	default:
		code = remoting.ReqSendMessageV2
	}
	request := remoting.CreateRequestCommand(code, header)
	request.SetBody(msg.Body)
	return request
}

// isReplyMessage reports MSG_TYPE == "reply".
func isReplyMessage(msg *common.Message) bool {
	v, ok := msg.GetProperty(common.PropertyMessageType)
	return ok && v == common.ReplyMessageFlag
}

// processSendResponse maps a SEND response onto a SendResult.
//
//   - msgId        = the client unique id (for a batch, the batch's own
//     UNIQ_KEY; inner-batch echoes it back in batchUniqId). When the broker
//     returns no batchUniqId (a client batch on an ordinary topic, not an
//     inner-batch), the batch's own id is used uniformly — the send here is a
//     single encoded batch message, so the parsing layer never sees the
//     sub-message list to join their ids. What matters for consumers and
//     traces: every sub-message carries its own UNIQ_KEY before encoding, so
//     the per-message ids the broker exposes are the client's own ids.
//   - offsetMsgId  = the header's msgId (the broker-generated offset id);
//   - regionId     = header MSG_REGION, defaulting to DefaultRegion;
//   - traceOn      = header TRACE_ON != "false" (the broker defaults to true).
func processSendResponse(response *remoting.RemotingCommand, msg *common.Message, mq common.MessageQueue) (*SendResult, error) {
	status, ok := sendStatusOf(response.Code)
	if !ok {
		return nil, common.BrokerError(response.Code, response.Remark)
	}
	header := &remoting.SendMessageResponseHeader{}
	header.FromExtFields(response.ExtFields())

	uniqMsgID := ""
	if v, ok := common.GetUniqID(msg); ok {
		uniqMsgID = v
	} else if header.BatchUniqID != nil {
		uniqMsgID = *header.BatchUniqID
	}
	if uniqMsgID == "" && header.MsgID != nil {
		uniqMsgID = *header.MsgID
	}

	queueID := mq.QueueID
	if header.QueueID != nil {
		queueID = *header.QueueID
	}
	result := &SendResult{
		SendStatus:   status,
		MsgID:        uniqMsgID,
		MessageQueue: common.NewMessageQueue(mq.Topic, mq.BrokerName, queueID),
		QueueOffset:  0,
		OffsetMsgID:  strValue(header.MsgID),
		TraceOn:      true,
		RecallHandle: strValue(header.RecallHandle),
	}
	if header.QueueOffset != nil {
		result.QueueOffset = *header.QueueOffset
	}
	if header.TransactionID != nil {
		result.TransactionID = *header.TransactionID
	}
	region := ""
	if response.ExtFields() != nil {
		if v, ok := response.ExtFields().Get(common.PropertyMsgRegion); ok {
			region = v
		}
	}
	if region == "" {
		region = common.DefaultTraceRegionID
	}
	result.RegionID = region
	if v, ok := response.ExtFields().Get(common.PropertyTraceSwitch); ok && v == "false" {
		result.TraceOn = false
	}
	return result, nil
}

// sendStatusOf maps a SEND response code to a SendStatus. Only these four codes
// mean "the broker accepted the message" in some form; everything else is a
// real error.
func sendStatusOf(code int32) (SendStatus, bool) {
	switch code {
	case remoting.RespSuccess:
		return SendOK, true
	case remoting.RespFlushDiskTimeout:
		return FlushDiskTimeout, true
	case remoting.RespFlushSlaveTimeout:
		return FlushSlaveTimeout, true
	case remoting.RespSlaveNotAvailable:
		return SlaveNotAvailable, true
	}
	return 0, false
}

// ---------------------------------------------------------------- send core

// sendAttempt performs ONE attempt against a concrete queue: resolve the
// master address, run the pre-send interceptors, build the request, invoke,
// parse — plus the SendMessageHook before/after pair around the RPC. The
// compression/sysFlag work is NOT here: the caller does it once
// outside the retry chain.
//
// Hook ordering:
//
//  1. resolve brokerAddr (findBrokerAddressInPublish, master only);
//  2. CheckForbiddenHook — error PROPAGATES, no SendMessageHook.after runs;
//  3. SendMessageHook.before (only if a send hook is registered; otherwise no
//     context is even allocated);
//  4. build the request and send;
//  5. SendMessageHook.after with either SendResult or Exception.
//
// One deliberate divergence: if a forbidden hook rejects the message AND a send
// hook is registered, the reference kernel would run
//
//	catch (...) { if (hasSendMessageHook()) { context.setException(e); ... } }
//
// with `context` still null — a NullPointerException that escapes the send
// entirely, because NPE is not one of the four caught types. This port skips
// the after-hook when there is no context instead of reproducing the NPE; the
// observable difference is that the caller gets the interceptor's error rather
// than a NullPointerException.
func (p *DefaultMQProducer) sendAttempt(msg *common.Message, isBatch bool, mq common.MessageQueue,
	sysFlag int32, timeoutMillis int64, mode CommunicationMode) (*SendResult, error) {

	inst, err := p.requireClient()
	if err != nil {
		return nil, err
	}
	addr, err := inst.PublishAddrFor(mq.BrokerName, mq.Topic)
	if err != nil {
		return nil, err
	}

	sendHooks, forbiddenHooks, _ := p.hookSnapshot()
	if err := p.checkForbidden(forbiddenHooks, msg, mq, addr, mode); err != nil {
		return nil, err
	}
	// W3C traceparent passthrough (opt-in): a message without one gets a root
	// span; a caller-propagated value is never overwritten. It runs in the same
	// position as the external OTel/SkyWalking hooks do elsewhere (after the
	// forbidden hooks, before the send context is built, so the trace hook sees
	// the property).
	if p.enableTraceContext {
		InjectTraceContext(msg)
	}

	var hookCtx *SendMessageContext
	if len(sendHooks) > 0 {
		hookCtx = p.buildSendContext(msg, mq, addr, mode)
		executeSendMessageHooksBefore(sendHooks, hookCtx)
	}

	request := p.buildSendRequest(msg, isBatch, mq, sysFlag)
	response, err := inst.Remoting().InvokeSync(p.sendAddr(addr), request, timeoutMillis)
	if err != nil {
		p.finishSendHook(sendHooks, hookCtx, nil, err)
		return nil, err
	}
	result, err := processSendResponse(response, msg, mq)
	p.finishSendHook(sendHooks, hookCtx, result, err)
	return result, err
}

// finishSendHook runs the after chain, if any. A nil context means no send hook
// was registered when the attempt started; see sendAttempt's last paragraph.
func (p *DefaultMQProducer) finishSendHook(hooks []SendMessageHook,
	ctx *SendMessageContext, result *SendResult, err error) {

	if ctx == nil {
		return
	}
	if err != nil {
		ctx.Exception = err
	} else {
		ctx.SendResult = result
	}
	executeSendMessageHooksAfter(hooks, ctx)
}

// sendOnewayTo performs a oneway send to a concrete queue. ONEWAY shares the
// send kernel with the sync path, so the interceptors and the send hooks run
// exactly as for a sync send (with ONEWAY in the context).
func (p *DefaultMQProducer) sendOnewayTo(msg *common.Message, mq common.MessageQueue, sysFlag int32) error {
	inst, err := p.requireClient()
	if err != nil {
		return err
	}
	// Oneway has no response, so the address must be resolved up front; the
	// kernel resolves master only.
	addr, err := inst.PublishAddrFor(mq.BrokerName, mq.Topic)
	if err != nil {
		return err
	}

	sendHooks, forbiddenHooks, _ := p.hookSnapshot()
	if err := p.checkForbidden(forbiddenHooks, msg, mq, addr, CommunicationModeOneway); err != nil {
		return err
	}
	// W3C traceparent passthrough (opt-in), same position as the sync path.
	if p.enableTraceContext {
		InjectTraceContext(msg)
	}

	var hookCtx *SendMessageContext
	if len(sendHooks) > 0 {
		hookCtx = p.buildSendContext(msg, mq, addr, CommunicationModeOneway)
		executeSendMessageHooksBefore(sendHooks, hookCtx)
	}

	request := p.buildSendRequest(msg, false, mq, sysFlag)
	request.MarkOnewayRPC()
	sendErr := inst.Remoting().InvokeOneway(p.sendAddr(addr), request)
	// A oneway send has no SendResult, so the after-hook sees a nil result on
	// success.
	p.finishSendHook(sendHooks, hookCtx, nil, sendErr)
	return sendErr
}

// updateFaultItem records one attempt's latency. The latency must be measured
// with a monotonic clock: a sub-millisecond local round trip measured with
// wall-clock milliseconds records as 0, and a 0 latency threshold would then
// never trigger.
func (p *DefaultMQProducer) updateFaultItem(brokerName string, began time.Time, isolation, reachable bool) {
	if brokerName == "" {
		return
	}
	p.faultStrategy.updateFaultItem(brokerName, float64(time.Since(began).Microseconds())/1000.0, isolation, reachable)
}

// sendDefaultImpl is the sync send with retry and fault avoidance.
// Errors are classified per type rather than "retry everything".
func (p *DefaultMQProducer) sendDefaultImpl(msg *common.Message, isBatch bool, timeoutMillis int64, sysFlag int32) (*SendResult, error) {
	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		// An unreachable route is immediately typed as
		// NOT_FOUND_TOPIC_EXCEPTION rather than burning the retry budget. An
		// error that ALREADY carries a code (10004 "no nameserver", decided in
		// validateNameServerSetting) is passed through unchanged — rewriting it
		// to 10005 would erase the distinction.
		code := common.NotFoundTopicException
		if existing, ok := responseCodeOf(err); ok {
			code = existing
		}
		return nil, common.ClientErrorCode(code, err.Error())
	}

	timesTotal := p.retryTimesWhenSendFail + 1
	beginFirst := time.Now()
	brokersSent := make([]string, 0, timesTotal)
	lastBrokerName := ""
	var result *SendResult
	var lastErr error
	callTimeout := false

	for attempt := 0; attempt < timesTotal; attempt++ {
		selected, ok, selErr := p.faultStrategy.selectOneMessageQueue(publish, lastBrokerName, attempt > 0)
		if selErr != nil {
			return nil, selErr
		}
		if !ok {
			break
		}
		lastBrokerName = selected.BrokerName
		brokersSent = append(brokersSent, selected.BrokerName)

		// The attempt clock starts right AFTER the queue is chosen, so
		// the queue-selection cost is not charged to the broker.
		began := time.Now()
		costTime := time.Since(beginFirst).Milliseconds()
		if costTime > timeoutMillis {
			callTimeout = true
			break
		}
		curTimeout := timeoutMillis - costTime
		canRetryAgain := attempt+1 < timesTotal
		if p.sendMsgMaxTimeoutPerReq > -1 && canRetryAgain && curTimeout > p.sendMsgMaxTimeoutPerReq {
			curTimeout = p.sendMsgMaxTimeoutPerReq
		}

		// `result` deliberately survives a failed attempt: a broker that
		// answered with a
		// non-retryable code returns the PREVIOUS attempt's result when there
		// was one, instead of throwing the error away.
		attemptResult, err := p.sendAttempt(msg, isBatch, selected, sysFlag, curTimeout, CommunicationModeSync)
		switch {
		case err == nil:
			result = attemptResult
			p.updateFaultItem(selected.BrokerName, began, false, true)
			// Only with retryAnotherBrokerWhenNotStoreOK does a non-SEND_OK
			// status move to another broker; otherwise the "stored, but not
			// cleanly" result is returned as-is.
			if result.SendStatus != SendOK && p.retryAnotherBrokerOK {
				lastErr = nil
				continue
			}
			return result, nil
		case common.IsKind(err, common.KindBroker):
			// The broker answered with an explicit code: isolate it (reachability
			// untouched); only retryable codes move on.
			p.updateFaultItem(selected.BrokerName, began, true, false)
			lastErr = err
			if code, _ := responseCodeOf(err); p.IsRetryResponseCode(code) {
				continue
			}
			if result != nil {
				return result, nil
			}
			return nil, err
		case common.IsKind(err, common.KindConnect), common.IsKind(err, common.KindTimeout),
			common.IsKind(err, common.KindSendRequest), common.IsKind(err, common.KindIO):
			p.updateFaultItem(selected.BrokerName, began, true, !p.faultStrategy.StartDetectorEnable())
			lastErr = err
		default:
			// Client-side problems (no queue selectable, route gone, decode
			// failure): record latency only, no isolation.
			p.updateFaultItem(selected.BrokerName, began, false, true)
			lastErr = err
		}
	}

	if result != nil {
		return result, nil
	}
	if callTimeout {
		return nil, common.TooMuchRequestError("sendDefaultImpl call timeout")
	}
	info := fmt.Sprintf("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: [%s], last error: %v",
		len(brokersSent), time.Since(beginFirst).Milliseconds(), msg.Topic,
		strings.Join(brokersSent, ", "), lastErr)
	var code int32
	hasCode := false
	switch {
	case common.IsKind(lastErr, common.KindBroker):
		code, hasCode = responseCodeOf(lastErr)
	case common.IsKind(lastErr, common.KindConnect):
		code, hasCode = common.ConnectBrokerException, true
	case common.IsKind(lastErr, common.KindTimeout):
		code, hasCode = common.AccessBrokerTimeout, true
	case common.IsKind(lastErr, common.KindClient):
		code, hasCode = common.BrokerNotExistException, true
	}
	if hasCode {
		out := common.ClientErrorCode(code, info)
		out.Cause = lastErr
		return nil, out
	}
	out := common.ClientError(info)
	out.Cause = lastErr
	return nil, out
}

// ---------------------------------------------------------------- send paths

// Send sends one message synchronously, round-robining across the topic's
// queues and retrying on other brokers per the retry rules.
func (p *DefaultMQProducer) Send(msg *common.Message) (*SendResult, error) {
	return p.SendWithTimeout(msg, p.sendMsgTimeout)
}

// SendWithTimeout is Send with an explicit timeout budget.
func (p *DefaultMQProducer) SendWithTimeout(msg *common.Message, timeoutMillis int64) (*SendResult, error) {
	if msg == nil {
		return nil, common.ClientError("the message is null")
	}
	if acc := p.accumulator(); acc != nil {
		return acc.Send(msg, p)
	}
	return p.sendDirectWithTimeout(msg, timeoutMillis)
}

// sendDirectWithTimeout is the non-batched sync send (the classic SendWithTimeout
// body). The send kernel's finally semantics restore the caller's message: the
// body goes back to the pre-compression one and the topic loses the namespace
// prefix. Skipping it breaks callers that reuse the same Message: the in-place
// compression would compress an already-compressed stream on the next send
// (zlib(zlib(x))), and the caller would keep seeing the rewritten `ns%topic`.
func (p *DefaultMQProducer) sendDirectWithTimeout(msg *common.Message, timeoutMillis int64) (*SendResult, error) {
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	if err := p.checkMessage(msg); err != nil {
		return nil, err
	}
	sysFlag := p.tryToCompressMessage(msg, false)
	return p.sendDefaultImpl(msg, false, timeoutMillis, sysFlag)
}

// SendToQueue sends a message to a PINNED queue.
func (p *DefaultMQProducer) SendToQueue(msg *common.Message, mq common.MessageQueue) (*SendResult, error) {
	return p.sendToQueueWithTimeout(msg, mq, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) sendToQueueWithTimeout(msg *common.Message, mq common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	if msg == nil {
		return nil, common.ClientError("the message is null")
	}
	if acc := p.accumulator(); acc != nil {
		return acc.SendWithMq(msg, mq, p)
	}
	return p.sendDirectToQueueWithTimeout(msg, mq, timeoutMillis)
}

func (p *DefaultMQProducer) sendDirectToQueueWithTimeout(msg *common.Message, mq common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	if err := p.checkMessage(msg); err != nil {
		return nil, err
	}
	// Fixed order: local validation -> pinned guard -> send kernel (compression
	// inside) -> timeout recheck. Placed before compression so a rejection
	// leaves the caller's message untouched.
	if err := p.checkPinnedTopic(msg.Topic, mq, pinnedTopicMismatchSync); err != nil {
		return nil, err
	}
	sysFlag := p.tryToCompressMessage(msg, false)
	return p.sendAttempt(msg, false, common.NewMessageQueue(mq.Topic, mq.BrokerName, mq.QueueID), sysFlag, timeoutMillis, CommunicationModeSync)
}

// SendBatch sends a batch as one SEND_BATCH_MESSAGE (request code 320) request.
func (p *DefaultMQProducer) SendBatch(messages []*common.Message) (*SendResult, error) {
	return p.sendBatch(messages, nil, p.sendMsgTimeout, pinnedTopicMismatchSync)
}

// SendBatchToQueue sends a batch to a pinned queue.
func (p *DefaultMQProducer) SendBatchToQueue(messages []*common.Message, mq common.MessageQueue) (*SendResult, error) {
	return p.sendBatch(messages, &mq, p.sendMsgTimeout, pinnedTopicMismatchSync)
}

func joinMemberUniqIDs(messages []*common.Message) string {
	ids := make([]string, 0, len(messages))
	for _, m := range messages {
		if u, ok := common.GetUniqID(m); ok {
			ids = append(ids, u)
		}
	}
	return strings.Join(ids, ",")
}

// sendBatch is the batch kernel. pinnedMismatch is the wording for the pinned
// queue guard: the async caller passes pinnedTopicMismatchAsync, because the
// async prologue uses its own text rather than the sync one.
func (p *DefaultMQProducer) sendBatch(messages []*common.Message, pinned *common.MessageQueue,
	timeoutMillis int64, pinnedMismatch string) (*SendResult, error) {
	if _, err := p.requireClient(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, common.ClientError("message list is empty")
	}
	// Fixed order per the batch contract: every sub-message runs local
	// validation (before namespacing, on the raw topic), then gets
	// its own UNIQ_KEY, and only then is the namespace applied. The batch
	// itself also needs a UNIQ_KEY (the broker uses it to detect an inner-batch),
	// and encoding comes last: get the order
	// wrong and the sub-messages are serialized without ids, so every message
	// the consumer sees lacks a client id.
	//
	// Skipping checkMessage would mean the batch path bypasses every local
	// validation — oversized bodies, empty bodies and illegal topics would all
	// go out on the wire.
	for _, m := range messages {
		if err := common.CheckMessage(m, p.maxMessageSize); err != nil {
			return nil, err
		}
		common.SetUniqID(m)
		m.Topic = p.withNamespace(m.Topic)
	}
	batch, err := common.GenerateFromList(messages)
	if err != nil {
		return nil, err
	}
	common.SetUniqID(batch.Message)
	batch.Message.Body = batch.Encode()

	if pinned != nil {
		// The batch goes through the same sync guard as a single message (a
		// batch IS a message for the pinned-topic check).
		if err := p.checkPinnedTopic(batch.Message.Topic, *pinned, pinnedMismatch); err != nil {
			return nil, err
		}
	}
	// tryToCompressMessage skips a batch outright, so batches are never
	// compressed.
	sysFlag := p.tryToCompressMessage(batch.Message, true)
	var res *SendResult
	var sendErr error
	if pinned != nil {
		res, sendErr = p.sendAttempt(batch.Message, true, *pinned, sysFlag, timeoutMillis, CommunicationModeSync)
	} else {
		publish, pubErr := p.topicPublishInfo(batch.Message.Topic)
		if pubErr != nil {
			return nil, pubErr
		}
		selected, ok, selErr := publish.SelectOneMessageQueue(nil)
		if selErr != nil {
			return nil, selErr
		}
		if !ok {
			return nil, common.ClientError("no message queue for publish info")
		}
		res, sendErr = p.sendAttempt(batch.Message, true, selected, sysFlag, timeoutMillis, CommunicationModeSync)
	}
	if sendErr != nil {
		return nil, sendErr
	}
	if len(messages) > 1 {
		res.MsgID = joinMemberUniqIDs(messages)
	}
	return res, nil
}

// sendAccumulatorBatch assembles the accumulated messages into ONE MessageBatch
// carrying the AggregateKey's tag and (sync-collected) keys, then sends it as a
// batch. It mirrors sendBatch but takes the batch-level tag/keys the accumulator
// owns, so a batch is never split across topics/tags on the wire. Sub-message
// topics are namespaced for the encode and restored afterwards so callers may
// reuse their Message objects.
func (p *DefaultMQProducer) sendAccumulatorBatch(messages []*common.Message, tag string, hasTag bool,
	keys map[string]struct{}, pinned *common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	if _, err := p.requireClient(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, common.ClientError("message list is empty")
	}
	for _, m := range messages {
		if err := common.CheckMessage(m, p.maxMessageSize); err != nil {
			return nil, err
		}
		common.SetUniqID(m)
	}
	prevTopics := make([]string, len(messages))
	for i, m := range messages {
		prevTopics[i] = m.Topic
		m.Topic = p.withNamespace(m.Topic)
	}
	defer func() {
		for i, m := range messages {
			m.Topic = prevTopics[i]
		}
	}()

	batch, err := common.GenerateFromList(messages)
	if err != nil {
		return nil, err
	}
	common.SetUniqID(batch.Message)
	if hasTag {
		batch.Message.SetTags(tag)
	}
	parts := make([]string, 0, len(keys))
	for k := range keys {
		parts = append(parts, k)
	}
	batch.Message.SetKeys(strings.Join(parts, common.KeySeparator))
	batch.Message.Body = batch.Encode()

	sysFlag := p.tryToCompressMessage(batch.Message, true)
	var res *SendResult
	if pinned != nil {
		if err := p.checkPinnedTopic(batch.Message.Topic, *pinned, pinnedTopicMismatchSync); err != nil {
			return nil, err
		}
		res, err = p.sendAttempt(batch.Message, true, *pinned, sysFlag, timeoutMillis, CommunicationModeSync)
	} else {
		var publish *TopicPublishInfo
		publish, err = p.topicPublishInfo(batch.Message.Topic)
		if err != nil {
			return nil, err
		}
		selected, ok, selErr := publish.SelectOneMessageQueue(nil)
		if selErr != nil {
			return nil, selErr
		}
		if !ok {
			return nil, common.ClientError("no message queue for publish info")
		}
		res, err = p.sendAttempt(batch.Message, true, selected, sysFlag, timeoutMillis, CommunicationModeSync)
	}
	if err != nil {
		return nil, err
	}
	if len(messages) > 1 {
		res.MsgID = joinMemberUniqIDs(messages)
	}
	return res, nil
}

// accumulator returns the live ProduceAccumulator, or nil when auto-batching is
// off or not yet started. Lock-free: the send hot path must not take p.mu.
func (p *DefaultMQProducer) accumulator() *ProduceAccumulator {
	if !p.autoBatchReady.Load() {
		return nil
	}
	return p.accumulatorPtr.Load()
}

// sendDirectWithMq is the direct (non-batched) sync send used as the
// accumulator's fallback for delayed/retry/gate-full messages.
func (p *DefaultMQProducer) sendDirectWithMq(msg *common.Message, mq *common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	if mq == nil {
		return p.sendDirectWithTimeout(msg, timeoutMillis)
	}
	return p.sendDirectToQueueWithTimeout(msg, *mq, timeoutMillis)
}

// sendAsyncDirect is the direct (non-batched) async send used as the
// accumulator's fallback; it dispatches straight to the async kernel, bypassing
// the accumulator (no recursion).
func (p *DefaultMQProducer) sendAsyncDirect(msg *common.Message, mq *common.MessageQueue, callback SendCallback, timeoutMillis int64) {
	req := &asyncRequest{msg: msg, callback: callback, timeout: timeoutMillis}
	if mq != nil {
		m := *mq
		req.mq = &m
	}
	_ = p.dispatchAsync(req)
}

// SetAutoBatch enables/disables ProduceAccumulator auto-batching. Call before Start.
func (p *DefaultMQProducer) SetAutoBatch(enable bool) {
	p.mu.Lock()
	p.autoBatch = enable
	p.mu.Unlock()
}

// SetBatchMaxDelayMs sets the per-batch hold time in ms (1..30000). Call before Start.
func (p *DefaultMQProducer) SetBatchMaxDelayMs(ms int64) error {
	if ms <= 0 || ms > 30*1000 {
		return common.ClientError("batchMaxDelayMs expect between 1ms and 30s, but get " + itoa64(ms) + "!")
	}
	p.mu.Lock()
	p.batchMaxDelayMs = ms
	p.mu.Unlock()
	return nil
}

// SetBatchMaxBytes sets the per-batch byte threshold (1..2MB). Call before Start.
func (p *DefaultMQProducer) SetBatchMaxBytes(n int64) error {
	if n <= 0 || n > 2*1024*1024 {
		return common.ClientError("batchMaxBytes expect between 1B and 2MB, but get " + itoa64(n) + "!")
	}
	p.mu.Lock()
	p.batchMaxBytes = n
	p.mu.Unlock()
	return nil
}

// SendOneway fires and forgets. There is no response, so the
// broker address must be resolved up front; pass a nil mq to round-robin.
func (p *DefaultMQProducer) SendOneway(msg *common.Message, mq *common.MessageQueue) error {
	if msg == nil {
		return common.ClientError("the message is null")
	}
	if _, err := p.requireClient(); err != nil {
		return err
	}
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	if err := p.checkMessage(msg); err != nil {
		return err
	}
	sysFlag := p.tryToCompressMessage(msg, false)

	if mq != nil {
		pinned := common.NewMessageQueue(mq.Topic, mq.BrokerName, mq.QueueID)
		if err := p.checkPinnedTopic(msg.Topic, pinned, pinnedTopicMismatchSync); err != nil {
			return err
		}
		return p.sendOnewayTo(msg, pinned, sysFlag)
	}
	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		return err
	}
	selected, ok, err := p.faultStrategy.selectOneMessageQueue(publish, "", false)
	if err != nil {
		return err
	}
	if !ok {
		return common.ClientError("no message queue for publish info")
	}
	return p.sendOnewayTo(msg, selected, sysFlag)
}

// SendOnewayBySelector is the SELECT/ONEWAY path: the caller's
// MessageQueueSelector picks the queue and the message goes out as a oneway
// send, with no response and no retry.
//
// The budget rule of the reference selector path still applies: the route
// lookup and the selector itself are measured against the producer's send
// timeout, and a selector that panics becomes "select message queue threw
// exception." instead of escaping through the send.
func (p *DefaultMQProducer) SendOnewayBySelector(msg *common.Message, selector MessageQueueSelector, arg any) error {
	return p.SendOnewayBySelectorWithTimeout(msg, selector, arg, p.sendMsgTimeout)
}

// SendOnewayBySelectorWithTimeout is SendOnewayBySelector with an explicit
// send budget. The budget only gates the route lookup plus the selection (a
// oneway send has no response to time out on).
func (p *DefaultMQProducer) SendOnewayBySelectorWithTimeout(msg *common.Message, selector MessageQueueSelector,
	arg any, timeoutMillis int64) error {

	if msg == nil {
		return common.ClientError("the message is null")
	}
	if selector == nil {
		return common.ClientError("message queue selector is null")
	}
	if _, err := p.requireClient(); err != nil {
		return err
	}
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	// The selector sees the ORIGINAL message (topic / business fields), so the
	// route lookup and the selection run before compression — exactly as on the
	// synchronous selector paths.
	selected, err := p.invokeMessageQueueSelector(msg, selector, arg, timeoutMillis)
	if err != nil {
		return err
	}
	if err := p.checkMessage(msg); err != nil {
		return err
	}
	sysFlag := p.tryToCompressMessage(msg, false)
	return p.sendOnewayTo(msg, selected, sysFlag)
}

// SendBySelector sends via a MessageQueueSelector. This path always hands the
// chosen queue straight to the send kernel.
func (p *DefaultMQProducer) SendBySelector(msg *common.Message, selector MessageQueueSelector, arg any) (*SendResult, error) {
	return p.SendBySelectorWithTimeout(msg, selector, arg, p.sendMsgTimeout)
}

// SendBySelectorWithTimeout is SendBySelector with an explicit timeout.
//
// Deliberately a SINGLE attempt to the selected queue: the selector path goes
// straight to the send kernel rather than the retry-capable default path, so
// the round-robin/retry machinery never runs. Retrying would also defeat the
// point of a selector (the caller chose the shard).
func (p *DefaultMQProducer) SendBySelectorWithTimeout(msg *common.Message, selector MessageQueueSelector, arg any, timeoutMillis int64) (*SendResult, error) {
	if msg == nil {
		return nil, common.ClientError("the message is null")
	}
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	// The selector sees the ORIGINAL message (topic/business fields);
	// compression only touches the body, and must therefore run after.
	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		return nil, err
	}
	selected, err := selector.Select(publish.MsgQueueList(), msg, arg)
	if err != nil {
		return nil, err
	}
	if err := p.checkMessage(msg); err != nil {
		return nil, err
	}
	sysFlag := p.tryToCompressMessage(msg, false)
	return p.sendAttempt(msg, false, selected, sysFlag, timeoutMillis, CommunicationModeSync)
}

// SendAsync and the rest of the asynchronous surface live in async.go — the
// chain is long enough (back-pressure gate, bounded executors, per-attempt retry
// with a reused request, callback executor) that keeping it here would bury the
// six synchronous send paths it sits next to.

// ---------------------------------------------------------------- transaction

// SendMessageInTransaction sends a transactional (half) message, in phases:
//
//  1. half message: stamp TRAN_MSG / PGROUP, send with sysFlag
//     TRANSACTION_PREPARED_TYPE. The broker records it in
//     RMQ_SYS_TRANS_HALF_TOPIC and it stays INVISIBLE to consumers;
//  2. local transaction: only on SEND_OK, results/exception collapse into a
//     LocalTransactionState;
//  3. END_TRANSACTION(37, oneway) tells the broker commit/rollback/unknown;
//  4. on UNKNOW (or a failed local transaction) the broker checks back with
//     CHECK_TRANSACTION_STATE(39), and handleCheckTransactionState calls
//     listener.CheckLocalTransaction and then ends the transaction.
//
// Where the check-back's listener comes from is the one place this port is a
// deliberate SUPERSET of the reference behaviour. The reference API resolves
// the check-back listener only for a TransactionMQProducer — a plain
// DefaultMQProducer's half messages are recorded by the broker but can never
// be answered (the check-back just logs "pick transactionCheckListener by
// group[..] failed"). Here the listener handed to
// this call is remembered and used for the check-back too, which makes the
// standalone (non-TransactionMQProducer) path actually work.
//
// A TransactionMQProducer's own listener still takes precedence: the wrapper
// installs a resolver that handleCheckTransactionState consults first, so
// SetTransactionListener keeps controlling the check-back even if a 3-arg send
// passed a different listener for its local transaction.
func (p *DefaultMQProducer) SendMessageInTransaction(msg *common.Message, listener TransactionListener, arg any) (*TransactionSendResult, error) {
	if listener == nil {
		return nil, common.ClientError("tranExecutor is null")
	}
	if msg == nil {
		return nil, common.ClientError("the message is null")
	}
	if _, err := p.requireClient(); err != nil {
		return nil, err
	}
	// The half message restores the caller's body as soon as it is sent, so
	// executeLocalTransaction sees the
	// ORIGINAL body (not the compressed stream). This cannot use a plain defer:
	// that would restore only after the method returns, and both the listener
	// and endTransaction would see the compressed message.
	prevBody := msg.Body
	msg.Topic = p.withNamespace(msg.Topic)

	// Protocol constraint: transactional messages support no
	// form of delayed delivery.
	for _, key := range delayProperties {
		if _, ok := msg.GetProperty(key); ok {
			return nil, common.ClientError("Transactional messages do not support delayed delivery")
		}
	}
	if err := p.checkMessage(msg); err != nil {
		return nil, err
	}

	// Half-message markers: the broker uses these to route the message into
	// RMQ_SYS_TRANS_HALF_TOPIC, and to find this producer for the check-back.
	msg.PutProperty(common.PropertyTransactionPrepared, "true")
	msg.PutProperty(common.PropertyProducerGroup, p.producerGroup)
	p.mu.Lock()
	p.transactionListener = listener
	p.mu.Unlock()

	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
		return nil, err
	}
	selected, ok, err := publish.SelectOneMessageQueue(nil)
	if err != nil {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
		return nil, err
	}
	if !ok {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
		return nil, common.ClientError("no message queue for publish info")
	}

	// Compression behaves as for a normal send (the transactional send goes
	// through the same send kernel), then the transaction bits are layered
	// on top, once TRAN_MSG is detected.
	sysFlag := p.tryToCompressMessage(msg, false)
	sysFlag = common.ResetTransactionValue(sysFlag, common.MessageSysFlagTransactionPrepared)

	// The half message is an ordinary SYNC send through the send kernel, so the
	// interceptors and the send hooks fire with CommunicationMode.SYNC and the
	// context classified as TransMsgHalf (TRAN_MSG=="true" is set above).
	sendResult, sendErr := p.sendAttempt(msg, false, selected, sysFlag, p.sendMsgTimeout, CommunicationModeSync)
	// Restored as soon as the HALF message is out, so executeLocalTransaction
	// and endTransaction below see the original body and the un-namespaced
	// topic — endTransaction puts msg.Topic on the wire, so it must carry the
	// caller's topic. Properties are NOT restored (the
	// UNIQ_KEY stays on the message by design).
	msg.Body = prevBody
	msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	if sendErr != nil {
		return nil, common.ClientError(fmt.Sprintf("send message Exception: %v", sendErr))
	}

	state := Unknow
	var localException error
	switch sendResult.SendStatus {
	case SendOK:
		if sendResult.TransactionID != "" {
			msg.PutProperty("__transactionId__", sendResult.TransactionID)
		}
		if uniq, ok := common.GetUniqID(msg); ok && uniq != "" {
			msg.TransactionID = uniq
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					// User code here must not kill the process: a Go panic is
					// the analogue of the caught exception, and is recovered.
					common.LogErrorf("executeLocalTransactionBranch exception, topic=%s: %v", msg.Topic, r)
					localException = fmt.Errorf("%v", r)
				}
			}()
			state = listener.ExecuteLocalTransaction(msg, arg)
		}()
	case FlushDiskTimeout, FlushSlaveTimeout, SlaveNotAvailable:
		state = RollbackMessage
	}

	// An endTransaction failure is only a warning; it does not change the
	// returned result.
	if err := p.endTransaction(sendResult, msg, state, localException, false,
		nil, nil, ""); err != nil {
		common.LogWarnf("local transaction execute %s, but end broker transaction failed: %v", state, err)
	}
	return &TransactionSendResult{SendResult: *sendResult, LocalTransactionState: state}, nil
}

// endTransaction sends END_TRANSACTION(37) oneway.
//
//   - the normal path (fromTransactionCheck == false) takes offsets and the
//     transaction id from the send result;
//   - the check-back path takes them from the broker's check request, because
//     no send result / mq exists any more; msgId comes from the message's
//     UNIQ_KEY.
func (p *DefaultMQProducer) endTransaction(sendResult *SendResult, msg *common.Message,
	state LocalTransactionState, localException error, fromTransactionCheck bool,
	checkHeader *remoting.CheckTransactionStateRequestHeader, msgExt *common.MessageExt,
	brokerAddr string) error {

	inst, err := p.requireClient()
	if err != nil {
		return err
	}
	header := &remoting.EndTransactionRequestHeader{ProducerGroup: strPtr(p.producerGroup)}

	// hookMsg / hookMsgID feed doExecuteEndTransactionHook: the id comes from
	// sendResult.MsgID on the caller path and from the message's
	// UNIQ_KEY (falling back to its msgId) on the check-back path.
	var hookMsg *common.Message
	var hookMsgID string

	if fromTransactionCheck {
		// The check request carries whatever the broker knew about the half
		// message (compression and transaction bookkeeping included).
		header.CommitLogOffset = checkHeader.CommitLogOffset
		header.TranStateTableOffset = checkHeader.TranStateTableOffset
		header.TransactionID = checkHeader.TransactionID
		header.Bname = checkHeader.Bname
		header.Topic = checkHeader.Topic
		if msgExt != nil {
			hookMsg = &msgExt.Message
			uniq := ""
			if v, ok := msgExt.GetProperty(common.PropertyUniqKey); ok {
				uniq = v
			}
			if uniq == "" {
				uniq = msgExt.MsgID
			}
			header.MsgID = strPtr(uniq)
			hookMsgID = uniq
		}
	} else {
		// The offset id carries the commitlog offset:
		// id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId).
		id := sendResult.OffsetMsgID
		if id == "" {
			id = sendResult.MsgID
		}
		_, _, offset, err := common.DecodeMessageID(id)
		if err != nil {
			return err
		}
		brokerName := sendResult.MessageQueue.BrokerName
		header.CommitLogOffset = i64Ptr(offset)
		header.TranStateTableOffset = i64Ptr(sendResult.QueueOffset)
		// An empty transaction id is left ABSENT, not written as "": the wire
		// writer skips nil fields, so an empty string on the wire would be a
		// third behaviour nobody expects.
		header.TransactionID = strPtrIfSet(sendResult.TransactionID)
		header.Bname = strPtr(brokerName)
		header.Topic = strPtr(msg.Topic)
		header.MsgID = strPtr(sendResult.MsgID)
		hookMsg = msg
		hookMsgID = sendResult.MsgID
		// The address must come from findBrokerAddressInPublish (master
		// only); the reference path sends oneway without a null check first —
		// this port keeps the explicit guard instead, but the address source
		// must be master-only all the same.
		addr, ok := inst.FindBrokerAddressInPublish(brokerName)
		if !ok {
			return common.ClientError("no broker address for end transaction")
		}
		brokerAddr = addr
	}

	header.CommitOrRollback = i32Ptr(transactionFlag(state))
	b := fromTransactionCheck
	header.FromTransactionCheck = &b

	// The end-transaction hook runs immediately before the oneway send, on
	// BOTH paths (caller-initiated and check-back) — so it
	// fires even if the send below then fails, and the trace it emits is the
	// only record that this transaction was finished at all.
	_, _, endTxHooks := p.hookSnapshot()
	p.doExecuteEndTransactionHook(endTxHooks, hookMsg, hookMsgID, brokerAddr, state, fromTransactionCheck)

	request := remoting.CreateRequestCommand(remoting.ReqEndTransaction, header)
	if localException != nil {
		request.Remark = fmt.Sprintf("executeLocalTransactionBranch exception: %v", localException)
	}
	if brokerAddr == "" {
		return common.ClientError("no broker address for end transaction")
	}
	return inst.Remoting().InvokeOneway(p.sendAddr(brokerAddr), request)
}

// transactionFlag maps LocalTransactionState to the MessageSysFlag wire
// commitOrRollback value.
func transactionFlag(state LocalTransactionState) int32 {
	switch state {
	case CommitMessage:
		return common.MessageSysFlagTransactionCommit // 0x2<<2 = 8
	case RollbackMessage:
		return common.MessageSysFlagTransactionRollback // 0x3<<2 = 12
	}
	return common.MessageSysFlagTransactionNotType // 0
}

// handleCheckTransactionState processes the broker's transaction check-back
// (CHECK_TRANSACTION_STATE = 39).
//
// The broker sends 39 as a ONEWAY request (body = the whole encoded MessageExt),
// so the client must NOT reply. Instead the listener runs OFF the remoting
// thread and the verdict goes back with END_TRANSACTION(fromTransactionCheck =
// true).
//
// Where it runs: on a TransactionMQProducer that has started, the whole check
// is submitted to the check executor (single worker by default), so check-backs
// are serialised. Everywhere else it
// falls back to a bare goroutine. In neither case may it run on the remoting
// read goroutine: it is user code and may block, which would stall every other
// response on that connection.
func (p *DefaultMQProducer) handleCheckTransactionState(request *remoting.RemotingCommand, addr string, _ *remoting.ResponseSink) {
	header := &remoting.CheckTransactionStateRequestHeader{}
	header.FromExtFields(request.ExtFields())

	if len(request.Body) == 0 {
		common.LogWarnf("checkTransactionState: empty body from %s", addr)
		return
	}
	msgExt, err := common.DecodeMessage(request.Body)
	if err != nil {
		common.LogWarnf("checkTransactionState: decode message failed: %v", err)
		return
	}
	if group, ok := msgExt.GetProperty(common.PropertyProducerGroup); ok && group != p.producerGroup {
		common.LogDebugf("checkTransactionState: group %s not mine (%s)", group, p.producerGroup)
		return
	}
	// Resolve the listener OUTSIDE p.mu: the TransactionMQProducer hook takes
	// its own lock, and initTransactionEnv takes that lock before p.mu, so
	// calling it while holding p.mu would be a lock-order inversion.
	p.mu.Lock()
	listenerFn := p.checkListenerFn
	stored := p.transactionListener
	executor := p.checkExecutor
	p.mu.Unlock()

	listener := stored
	if listenerFn != nil {
		if l := listenerFn(); l != nil {
			listener = l
		}
	}
	if listener == nil {
		common.LogWarnf("checkTransactionState: no transaction listener for group %s", p.producerGroup)
		return
	}

	task := func() {
		state := Unknow
		var checkErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogErrorf("Broker call checkTransactionState, but checkLocalTransaction exception: %v", r)
					state = Unknow
					checkErr = fmt.Errorf("%v", r)
				}
			}()
			state = listener.CheckLocalTransaction(msgExt)
		}()
		if err := p.endTransaction(nil, nil, state, checkErr, true, header, msgExt, addr); err != nil {
			common.LogWarnf("checkTransactionState: end transaction failed: %v", err)
		}
	}

	// The whole check is submitted to the check executor,
	// whose default is a SINGLE worker with a 2000-slot queue: check-backs are
	// serialised and a flood is rejected. Reproducing that ordering matters — a
	// producer whose checkLocalTransaction is slow must not be able to run
	// check-backs concurrently with itself.
	//
	// The reference path lets the executor rejection escape checkTransactionState
	// and land on the remoting thread. This port logs it
	// and drops the check: the broker will re-check after
	// transactionCheckMax, whereas an escaping panic on the read goroutine would
	// take the whole connection's dispatch loop with it.
	if executor != nil {
		if err := executor.Submit(task); err != nil {
			common.LogErrorf("checkTransactionState: check executor rejected the request: %v", err)
		}
		return
	}
	go task()
}

// RecallMessage deletes a not-yet-delivered timer/delay message identified by
// the recall handle the original send returned, and returns its uniqKey (the
// namespace is applied at the facade). Fixed validation order:
// state, topic name, %RETRY%/%DLQ% rejection, handle decode, route refresh,
// broker pinning. The broker side gates this behind recallMessageEnable
// (default false), which surfaces as NO_PERMISSION.
func (p *DefaultMQProducer) RecallMessage(topic, recallHandle string) (string, error) {
	inst, err := p.requireClient()
	if err != nil {
		return "", err
	}
	topic = p.withNamespace(topic)
	if err := common.CheckTopic(topic); err != nil {
		return "", err
	}
	if common.IsRetryTopic(topic) || common.IsDLQTopic(topic) {
		return "", common.ClientError("topic is not supported")
	}
	handle, err := common.DecodeRecallHandle(recallHandle)
	if err != nil {
		return "", err
	}
	// The route fetch itself is unused, but its error must propagate
	// (a topic with no route must fail the recall here, not later).
	if _, err := p.topicPublishInfo(topic); err != nil {
		return "", err
	}
	// Master publish address first; any address of the topic's route (slave
	// allowed) as the fallback.
	addr, ok := inst.FindBrokerAddressInPublish(handle.BrokerName)
	if !ok {
		// The topic-route fallback yields nothing when the route has no
		// address — the error is swallowed and the miss reported below.
		if fallback, err := inst.BrokerAddrForTopic(topic); err == nil {
			addr, ok = fallback, true
		}
	}
	if !ok {
		common.LogWarnf("can't find broker service address. %s", handle.BrokerName)
		return "", common.ClientError("The broker service address not found")
	}
	request := remoting.CreateRequestCommand(remoting.ReqRecallMessage, &remoting.RecallMessageRequestHeader{
		ProducerGroup: strPtr(p.producerGroup),
		Topic:         strPtr(topic),
		RecallHandle:  strPtr(recallHandle),
		Bname:         strPtr(handle.BrokerName),
	})
	response, err := inst.Remoting().InvokeSync(p.sendAddr(addr), request, p.sendMsgTimeout)
	if err != nil {
		return "", err
	}
	if err := inst.checkResponse(response); err != nil {
		return "", err
	}
	var header remoting.RecallMessageResponseHeader
	header.FromExtFields(response.ExtFields())
	if strValue(header.MsgID) == "" {
		return "", common.ClientError(fmt.Sprintf("recall message response has no msgId, addr %s", addr))
	}
	return *header.MsgID, nil
}

// ---------------------------------------------------------------- misc

// FetchPublishMessageQueues returns the topic's publish queues from the route.
func (p *DefaultMQProducer) FetchPublishMessageQueues(topic string) ([]common.MessageQueue, error) {
	inst, err := p.requireClient()
	if err != nil {
		return nil, err
	}
	info, err := inst.GetTopicPublishInfo(p.withNamespace(topic), false)
	if err != nil {
		return nil, err
	}
	return info.MsgQueueList(), nil
}

// The tiny pointer helpers keep the header literals readable.
func strPtr(s string) *string { return &s }
func i32Ptr(v int32) *int32   { return &v }
func i64Ptr(v int64) *int64   { return &v }
func boolPtr(b bool) *bool    { return &b }

// strPtrIfSet returns nil for an empty string, so the field stays absent from
// extFields instead of appearing as "".
func strPtrIfSet(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func strValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// parseInt32 parses a message property into a header int32; a non-numeric value
// leaves the header field unset — a malformed property is not worth failing an
// otherwise valid send over.
func parseInt32(raw string) (int32, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}
