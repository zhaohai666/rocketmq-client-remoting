// DefaultMQProducer — the client-side send API (Java
// org.apache.rocketmq.client.producer.DefaultMQProducer /
// Python rocketmq.client.producer.DefaultMQProducer).
//
// Scope of this file: lifecycle, the six send paths (sync, to-queue, batch,
// oneway, selector, transaction) plus the retry/fault-avoidance loop, the send
// header assembly, the SEND response mapping and the broker's transaction
// check-back. Send latency fault tolerance lives in fault_strategy.go.
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

// Java DefaultMQProducer defaults.
const (
	DefaultSendMsgTimeout             = int64(3000)
	DefaultCompressMsgBodyOverHowmuch = 1024 * 4
	DefaultCompressLevel              = 5
	DefaultRetryTimesWhenSendFailed   = 2
	DefaultMaxMessageSize             = 1024 * 1024 * 4
	DefaultHeartbeatIntervalMillis    = int64(30_000)
	defaultPollNameServerInterval     = int64(30_000)
)

// Pinned-send topic guards (Java DefaultMQProducerImpl:1234-1236 sync,
// :1277-1278 async — deliberately two different texts).
const (
	pinnedTopicMismatchSync  = "message's topic not equal mq's topic"
	pinnedTopicMismatchAsync = "Topic of the message does not match its target message queue"
)

// delayProperties are the delay/timer property keys a transactional message may
// not carry (Java ensureNotDelayedForTransactional). Python reads the 5.x
// TIMER_* keys through getattr with a sentinel, so they never match; Go has no
// such constants either, which makes the two lists equivalent in practice.
var delayProperties = []string{
	common.PropertyDelayTimeLevel,
	common.PropertyDelayTime,
}

// ---------------------------------------------------------------- selectors

// MessageQueueSelector mirrors Java MessageQueueSelector / Python
// MessageQueueSelector.
type MessageQueueSelector interface {
	Select(mqs []common.MessageQueue, msg *common.Message, arg any) (common.MessageQueue, error)
}

// SelectMessageQueueByHash picks by arg's hash (Java SelectMessageQueueByHash).
//
// Python uses the builtin hash(arg), whose string hash changes with
// PYTHONHASHSEED and is not reproducible across processes. Like the Rust, C++
// and .NET ports this uses Java String.hashCode semantics instead —
// deterministic, which is the point of a sharding key.
type SelectMessageQueueByHash struct{}

func (SelectMessageQueueByHash) Select(mqs []common.MessageQueue, _ *common.Message, arg any) (common.MessageQueue, error) {
	if len(mqs) == 0 {
		return common.MessageQueue{}, common.ClientError("no message queue")
	}
	hash := common.JavaStringHash(fmt.Sprintf("%v", arg))
	// Java's Math.abs(Integer.MIN_VALUE) is still negative; collapse it to 0
	// explicitly (the C++ port does the same).
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

// SendCallback mirrors Java SendCallback. OnSuccess and OnException are
// mutually exclusive and each fires exactly once.
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

// ---------------------------------------------------------------- transaction

// TransactionListener mirrors Java TransactionListener / Python
// TransactionListener.
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

// DefaultMQProducer mirrors Java DefaultMQProducer.
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
	pollNameServerIntv int64

	sendLatencyFaultEnable bool
	faultStrategy          *mqFaultStrategy

	mqClient *Instance
	started  bool

	transactionListener TransactionListener

	heartbeatIntervalMillis int64
	heartbeatRunning        atomic.Bool
}

// NewDefaultMQProducer validates the group and applies the Java defaults.
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
		compressMsgBodyOverHow:  DefaultCompressMsgBodyOverHowmuch,
		compressLevel:           DefaultCompressLevel,
		compressType:            common.ZlibType,
		retryTimesWhenSendFail:  DefaultRetryTimesWhenSendFailed,
		sendMsgMaxTimeoutPerReq: -1,
		maxMessageSize:          DefaultMaxMessageSize,
		pollNameServerIntv:      defaultPollNameServerInterval,
		heartbeatIntervalMillis: DefaultHeartbeatIntervalMillis,
		retryResponseCodes: map[int32]struct{}{
			// Java DefaultMQProducerImpl.RetryResponseCodes: only these codes
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
func (p *DefaultMQProducer) SetPollNameServerIntervalMillis(v int64) {
	p.pollNameServerIntv = v
}
func (p *DefaultMQProducer) SetHeartbeatIntervalMillis(v int64) { p.heartbeatIntervalMillis = v }
func (p *DefaultMQProducer) SetSendMsgTimeout(v int64)          { p.sendMsgTimeout = v }
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

// SetSendLatencyFaultEnable toggles the fault strategy. Java allows flipping it
// at runtime, so it is not a start-time-only setting.
func (p *DefaultMQProducer) SetSendLatencyFaultEnable(enable bool) {
	p.faultStrategy.SetSendLatencyFaultEnable(enable)
}

func (p *DefaultMQProducer) IsSendLatencyFaultEnable() bool {
	return p.faultStrategy.SendLatencyFaultEnable()
}

// AddRetryResponseCode extends the retryable broker-code set.
func (p *DefaultMQProducer) AddRetryResponseCode(code int32) {
	p.retryResponseCodes[code] = struct{}{}
}

func (p *DefaultMQProducer) IsRetryResponseCode(code int32) bool {
	_, ok := p.retryResponseCodes[code]
	return ok
}

// SetNameServerAddr replaces the nameserver list. The Java form is a
// semicolon-separated string; empty entries are dropped.
func (p *DefaultMQProducer) SetNameServerAddr(addrs string) {
	p.nameServerAddrs = splitNamesrvAddr(addrs)
}

// SetNameServerAddresses sets the nameserver list directly.
func (p *DefaultMQProducer) SetNameServerAddresses(addrs []string) {
	p.nameServerAddrs = append([]string(nil), addrs...)
}

// splitNamesrvAddr mirrors Java's parsing: split on ';' and trim.
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

// Start mirrors Java DefaultMQProducerImpl.start: validate the group (after
// namespacing), build the clientId, bring up the shared client instance, then
// register the producer group and start the heartbeat thread.
//
// The heartbeat is NOT cosmetic: the broker's transaction check-back looks the
// producer group up in its channel table, which is filled from these
// heartbeats. Without it COMMIT/ROLLBACK still work (the client pushes
// END_TRANSACTION itself) but an UNKNOW half message is never checked back.
func (p *DefaultMQProducer) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil
	}
	// Java DefaultMQProducer.start:375 setProducerGroup(withNamespace(group)) —
	// the broker registers the PREFIXED group name. checkConfig runs after it.
	if p.namespace != "" {
		p.producerGroup = common.WrapNamespace(p.namespace, p.producerGroup)
	}
	if err := common.CheckGroup(p.producerGroup); err != nil {
		return err
	}
	if p.producerGroup == common.DefaultProducerGroup {
		return common.ClientError(fmt.Sprintf(
			"producerGroup can not equal %s, please specify another one.", common.DefaultProducerGroup))
	}
	if len(p.nameServerAddrs) == 0 {
		// Java only reports this on the first send (as 10004); surfacing it
		// here is more predictable, but must use the SAME code so callers do
		// not have to handle two different values for one failure.
		return common.ClientErrorCode(common.NoNameServerException, "name server address is not set")
	}
	// Java defaultMQProducerImpl.start:250-252: changeInstanceNameToPID then
	// buildMQClientId. The instance name is written back in place, so a second
	// Start() reuses the same clientId instead of minting a new one.
	p.instanceName = common.ChangeInstanceNameToPID(p.instanceName)
	if p.clientID == "" {
		p.clientID = common.ClientIDFor(p.instanceName, p.unitName, p.enableStreamRequestType)
	}
	cfg := NewClientInstanceConfig()
	if p.tlsEnable != nil {
		cfg.TLSEnable = *p.tlsEnable
	}
	cfg.PollNameServerIntervalMillis = p.pollNameServerIntv
	inst := CreateOrGetInstance(p.clientID, p.nameServerAddrs, cfg)
	// Namespace + stream + user(ACL) + zone, in Java's order and only once per
	// instance — the signature has to cover exactly the fields Java signs.
	inst.EnsureRPCHooks(p.namespaceV2, p.enableStreamRequestType, p.rpcHook)
	if err := inst.Start(); err != nil {
		return err
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
	// the first. Java sidesteps this with a single ClientRemotingProcessor that
	// dispatches by group; the Python port has the same limitation. Producers
	// started without SetInstanceName get a unique clientId each, so the
	// default configuration is unaffected.
	inst.Remoting().RegisterProcessor(remoting.ReqCheckTransactionState, p.handleCheckTransactionState)
	// Java start():257 registerProducer — the factory's shutdown guard needs it.
	inst.RegisterProducer(p.producerGroup)
	for _, topic := range p.topics {
		inst.RegisterTopicInUse(topic)
	}
	p.mqClient = inst
	p.started = true
	p.heartbeatRunning.Store(true)
	go p.heartbeatLoop()
	return nil
}

// Shutdown mirrors Java DefaultMQProducerImpl.shutdown.
func (p *DefaultMQProducer) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return
	}
	p.heartbeatRunning.Store(false)
	if p.mqClient != nil {
		// Graceful unregister (UNREGISTER_CLIENT = 35) per broker BEFORE the
		// transport goes away: the broker's ProducerManager state is per broker,
		// and without this it only expires via the channel scan (~120s), during
		// which the broker still considers the group connected — and would pick
		// this dead channel for a transaction check-back.
		p.mqClient.UnregisterClientAllBrokers(p.producerGroup, "")
		// Ordering as in Java: unregisterProducer, then shutdown the factory.
		p.mqClient.UnregisterProducer(p.producerGroup)
		p.mqClient.Shutdown()
		p.mqClient.DetachFromRegistryIfLastTenant()
	}
	p.mqClient = nil
	p.started = false
}

// heartbeatLoop periodically advertises this producer group to every broker
// (Java ProducerData registration).
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

// checkMessage mirrors Java Validators.checkMessage(msg, this) — purely local,
// runs before any network I/O.
func (p *DefaultMQProducer) checkMessage(msg *common.Message) error {
	return common.CheckMessage(msg, p.maxMessageSize)
}

// withNamespace mirrors Java ClientConfig.withNamespace.
func (p *DefaultMQProducer) withNamespace(topic string) string {
	if p.namespace == "" {
		return topic
	}
	return common.WrapNamespace(p.namespace, topic)
}

// validateNameServerSetting mirrors Java
// DefaultMQProducerImpl.validateNameServerSetting. It only runs on the
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

// topicPublishInfo mirrors Java DefaultMQProducerImpl.tryToFindTopicPublishInfo:
// fetch the REAL route first, and only when that genuinely fails (a brand new
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

// checkPinnedTopic mirrors Java DefaultMQProducerImpl:1234-1236 (sync) /
// :1277-1278 (async). It compares the two names AFTER namespacing each side
// (Java wraps the message topic and then queues the mq through
// ClientConfig.queueWithNamespace), because wrapNamespace is idempotent.
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
// Aligned with Java DefaultMQProducerImpl.tryToCompressMessage line by line:
//   - batch messages are NEVER compressed;
//   - the threshold is compressMsgBodyOverHowmuch (default 4096), compared with
//     >= (Java: body.length >= ...);
//   - a compression failure degrades to "not compressed" with a log line, it
//     does not fail the send;
//   - the compressed size is NOT compared against the original (Java does not
//     either).
//
// Callers must invoke this ONCE, outside the retry loop, and restore the body
// afterwards (every call site keeps prevBody and defers the restore).
//
// Java calls it inside sendKernelImpl, i.e. once per ATTEMPT, not once per
// send. That is NOT a behavioural difference: sendKernelImpl captures
// `byte[] prevBody = msg.getBody()` at :930 and its `finally` does
// `msg.setBody(prevBody)` at :1095, so every retry re-compresses the ORIGINAL
// body. Compressing once here and restoring at the end yields byte-identical
// payloads and sysFlag, and skips the redundant recompression of a body that
// is already compressed. (An earlier version of this comment claimed Java
// double-compressed into zlib(zlib(x)) on retry — it does not; the finally
// block prevents it.)
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

// buildSendRequest mirrors Java MQClientAPIImpl.sendMessage +
// DefaultMQProducerImpl.sendKernelImpl's header section.
//
// Rebuilt per attempt on purpose (broker name and queue id change on retry),
// which is also how Java behaves — including its quirk that the %RETRY% header
// hoisting below only fires on the first attempt, because the source
// properties were cleared in place.
func (p *DefaultMQProducer) buildSendRequest(msg *common.Message, isBatch bool, mq common.MessageQueue, sysFlag int32) *remoting.RemotingCommand {
	// Java sendKernelImpl:932-935: non-batch messages get a client-side unique
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
		// MAX_RECONSUME_TIMES property (Java sendKernelImpl:1003-1018). With a
		// client version >= V3_4_9 the broker honours this field
		// unconditionally (AbstractSendMessageProcessor:172-179), so always
		// sending 0 makes a retry message land in %DLQ% on its very first
		// delivery (reconsumeTimes(0) >= 0).
		MaxReconsumeTimes: nil,
		Batch:             boolPtr(isBatch),
		// Java sendKernelImpl:1007 requestHeader.setBrokerName(brokerName) —
		// the V2 extFields key is the single letter "n". Which broker this is
		// comes from the selected route queue.
		BrokerName: strPtr(mq.BrokerName),
	}
	// Java sendKernelImpl:1004-1018: for `%RETRY%<group>` two retry properties
	// must be HOISTED into the request header, because the broker's
	// handleRetryAndDLQ (SendMessageProcessor:197-210) reads
	// requestHeader.reconsumeTimes / maxReconsumeTimes rather than the message
	// properties. Missing the hoist is not an error but a SILENT misjudgement:
	// the broker falls back to the subscription group's retryMaxTimes (16) and
	// a message that should have gone to the DLQ after three tries keeps
	// circulating on the retry topic.
	//
	// The clear only affects this local message; the serialized properties
	// above already captured RECONSUME_TIME. Java has the same ordering — do
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
	// Batch messages go out as SEND_BATCH_MESSAGE(320); Java's test order is
	// reply first, then batch (MQClientAPIImpl:562).
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

// isReplyMessage mirrors Python is_reply_message: MSG_TYPE == "reply".
func isReplyMessage(msg *common.Message) bool {
	v, ok := msg.GetProperty(common.PropertyMessageType)
	return ok && v == common.ReplyMessageFlag
}

// processSendResponse mirrors Java MQClientAPIImpl.processSendResponse.
//
//   - msgId        = the client unique id (for a batch, the batch's own
//     UNIQ_KEY; inner-batch echoes it back in batchUniqId). Deliberate
//     divergence from Java: when the broker returns no batchUniqId (a client
//     batch on an ordinary topic, not an inner-batch), Java substitutes the
//     comma-joined per-message UNIQ_KEYs. This port's parsing layer only sees
//     the one message that was sent, not the sub-message list (Rust and C++
//     have the same limitation), and all four ports must be comparable — so
//     the batch's own id is used uniformly. The half that matters IS aligned:
//     every sub-message carries its own UNIQ_KEY before encoding, so the
//     per-message ids the broker exposes to consumers and traces are identical
//     to Java's.
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
// master address, build the request, invoke, parse.
func (p *DefaultMQProducer) sendAttempt(msg *common.Message, isBatch bool, mq common.MessageQueue, sysFlag int32, timeoutMillis int64) (*SendResult, error) {
	inst, err := p.requireClient()
	if err != nil {
		return nil, err
	}
	addr, err := inst.PublishAddrFor(mq.BrokerName, mq.Topic)
	if err != nil {
		return nil, err
	}
	request := p.buildSendRequest(msg, isBatch, mq, sysFlag)
	response, err := inst.Remoting().InvokeSync(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return processSendResponse(response, msg, mq)
}

// sendOnewayTo performs a oneway send to a concrete queue.
func (p *DefaultMQProducer) sendOnewayTo(msg *common.Message, mq common.MessageQueue, sysFlag int32) error {
	inst, err := p.requireClient()
	if err != nil {
		return err
	}
	// Oneway has no response, so the address must be resolved up front; the
	// discipline is Java sendKernelImpl's (master only).
	addr, err := inst.PublishAddrFor(mq.BrokerName, mq.Topic)
	if err != nil {
		return err
	}
	request := p.buildSendRequest(msg, false, mq, sysFlag)
	request.MarkOnewayRPC()
	return inst.Remoting().InvokeOneway(addr, request)
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

// sendDefaultImpl is the sync send with retry and fault avoidance (Java
// DefaultMQProducerImpl.sendDefaultImpl). Errors are classified per type rather
// than "retry everything".
func (p *DefaultMQProducer) sendDefaultImpl(msg *common.Message, isBatch bool, timeoutMillis int64, sysFlag int32) (*SendResult, error) {
	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		// Java: an unreachable route is immediately typed as
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

		// Java resets beginTimestampPrev right AFTER the queue is chosen, so
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

		// `result` deliberately survives a failed attempt (Java declares
		// sendResult outside the loop): a broker that answered with a
		// non-retryable code returns the PREVIOUS attempt's result when there
		// was one, instead of throwing the error away.
		attemptResult, err := p.sendAttempt(msg, isBatch, selected, sysFlag, curTimeout)
		switch {
		case err == nil:
			result = attemptResult
			p.updateFaultItem(selected.BrokerName, began, false, true)
			// Java: only with retryAnotherBrokerWhenNotStoreOK does a non-SEND_OK
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
			// Connection refused / timeout / write failure: isolate. This port
			// has no background reachability detector, so Java's
			// reachable = !isStartDetectorEnable() is always true.
			p.updateFaultItem(selected.BrokerName, began, true, true)
			lastErr = err
		default:
			// Client-side problems (no queue selectable, route gone, decode
			// failure): Java records latency only, no isolation.
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
	// Java's sendKernelImpl finally clause restores the caller's message: the
	// body goes back to the pre-compression one and the topic loses the
	// namespace prefix. Skipping it breaks callers that reuse the same Message:
	// the in-place compression would compress an already-compressed stream on
	// the next send (zlib(zlib(x))), and the caller would keep seeing the
	// rewritten `ns%topic`.
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

// SendToQueue sends a message to a PINNED queue (Java send(msg, mq, timeout)).
func (p *DefaultMQProducer) SendToQueue(msg *common.Message, mq common.MessageQueue) (*SendResult, error) {
	return p.sendToQueueWithTimeout(msg, mq, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) sendToQueueWithTimeout(msg *common.Message, mq common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	if msg == nil {
		return nil, common.ClientError("the message is null")
	}
	prevBody := msg.Body
	defer func() {
		msg.Body = prevBody
		msg.Topic = common.WithoutNamespace(msg.Topic, p.namespace)
	}()

	msg.Topic = p.withNamespace(msg.Topic)
	if err := p.checkMessage(msg); err != nil {
		return nil, err
	}
	// Java's order: Validators -> pinned guard -> sendKernelImpl (compression
	// inside) -> timeout recheck. Placed before compression so a rejection
	// leaves the caller's message untouched.
	if err := p.checkPinnedTopic(msg.Topic, mq, pinnedTopicMismatchSync); err != nil {
		return nil, err
	}
	sysFlag := p.tryToCompressMessage(msg, false)
	return p.sendAttempt(msg, false, common.NewMessageQueue(mq.Topic, mq.BrokerName, mq.QueueID), sysFlag, timeoutMillis)
}

// SendBatch mirrors Java DefaultMQProducer.batch (SEND_BATCH_MESSAGE = 320).
func (p *DefaultMQProducer) SendBatch(messages []*common.Message) (*SendResult, error) {
	return p.sendBatch(messages, nil, p.sendMsgTimeout)
}

// SendBatchToQueue sends a batch to a pinned queue.
func (p *DefaultMQProducer) SendBatchToQueue(messages []*common.Message, mq common.MessageQueue) (*SendResult, error) {
	return p.sendBatch(messages, &mq, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) sendBatch(messages []*common.Message, pinned *common.MessageQueue, timeoutMillis int64) (*SendResult, error) {
	if _, err := p.requireClient(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, common.ClientError("message list is empty")
	}
	// Java DefaultMQProducer.batch():1172-1184 — every sub-message runs
	// Validators.checkMessage (before namespacing, on the raw topic), then gets
	// its own UNIQ_KEY, and only then is the namespace applied. The batch
	// itself also needs a UNIQ_KEY (the broker uses it to detect an inner-batch
	// at SendMessageProcessor:617), and encoding comes last: get the order
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
		// Java routes the batch through the same sync guard as a single message
		// (MessageBatch extends Message).
		if err := p.checkPinnedTopic(batch.Message.Topic, *pinned, pinnedTopicMismatchSync); err != nil {
			return nil, err
		}
	}
	// tryToCompressMessage skips a batch outright, so batches are never
	// compressed.
	sysFlag := p.tryToCompressMessage(batch.Message, true)
	if pinned != nil {
		return p.sendAttempt(batch.Message, true, *pinned, sysFlag, timeoutMillis)
	}
	publish, err := p.topicPublishInfo(batch.Message.Topic)
	if err != nil {
		return nil, err
	}
	selected, ok, err := publish.SelectOneMessageQueue(nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, common.ClientError("no message queue for publish info")
	}
	return p.sendAttempt(batch.Message, true, selected, sysFlag, timeoutMillis)
}

// SendOneway fires and forgets (Java sendOneway). There is no response, so the
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

// SendBySelector sends via a MessageQueueSelector (Java send(msg, selector,
// arg)). Java always sends this path through sendKernelImpl, so it retries.
func (p *DefaultMQProducer) SendBySelector(msg *common.Message, selector MessageQueueSelector, arg any) (*SendResult, error) {
	return p.SendBySelectorWithTimeout(msg, selector, arg, p.sendMsgTimeout)
}

// SendBySelectorWithTimeout is SendBySelector with an explicit timeout.
//
// Deliberately a SINGLE attempt to the selected queue: Java's sendSelectImpl
// goes straight to sendKernelImpl rather than sendDefaultImpl, so the
// round-robin/retry machinery never runs. Retrying would also defeat the point
// of a selector (the caller chose the shard).
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
	return p.sendAttempt(msg, false, selected, sysFlag, timeoutMillis)
}

// SendAsync sends without blocking; callback fires exactly once.
//
// Deliberate simplifications versus Java/Python, none of which change the wire
// protocol:
//   - there is no dedicated async sender thread pool, just a goroutine, so
//     there is no bounded queue behind it;
//   - the async back-pressure semaphores (semaphoreAsyncSendNum /
//     ...SendSize) are not ported, so an unbounded number of sends may be in
//     flight.
//
// The retry rules DO apply, because this reuses the sync implementation — and
// they are the sync ones, which is what Java does too for the "broker answered
// an error code" case (no other broker is tried).
func (p *DefaultMQProducer) SendAsync(msg *common.Message, callback SendCallback) {
	if callback == nil {
		return
	}
	go func() {
		result, err := p.SendWithTimeout(msg, p.sendMsgTimeout)
		if err != nil {
			callback.OnException(err)
			return
		}
		callback.OnSuccess(result)
	}()
}

// ---------------------------------------------------------------- transaction

// SendMessageInTransaction sends a transactional (half) message. Java
// DefaultMQProducerImpl.sendMessageInTransaction, the two phases:
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
	// Java sendKernelImpl:930 keeps prevBody: the half message restores the
	// caller's body as soon as it is sent, so executeLocalTransaction sees the
	// ORIGINAL body (not the compressed stream). This cannot use a plain defer:
	// that would restore only after the method returns, and both the listener
	// and endTransaction would see the compressed message.
	prevBody := msg.Body
	msg.Topic = p.withNamespace(msg.Topic)

	// Java ensureNotDelayedForTransactional: transactional messages support no
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
	// through the same sendKernelImpl), then the transaction bits are layered
	// on top (Java :951-953, after detecting TRAN_MSG).
	sysFlag := p.tryToCompressMessage(msg, false)
	sysFlag = common.ResetTransactionValue(sysFlag, common.MessageSysFlagTransactionPrepared)

	sendResult, sendErr := p.sendAttempt(msg, false, selected, sysFlag, p.sendMsgTimeout)
	// Same point as Java's finally (sendKernelImpl:1095-1096): restored as soon
	// as the HALF message is out, so executeLocalTransaction and endTransaction
	// below see the original body and the un-namespaced topic (Java 5.5.1's
	// endTransaction:1543 uses msg.getTopic()). Properties are NOT restored —
	// Java does not restore UNIQ_KEY either.
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
					// Java catches Exception here; a Go panic from user code is
					// the analogue, and must not kill the process.
					common.LogErrorf("executeLocalTransactionBranch exception, topic=%s: %v", msg.Topic, r)
					localException = fmt.Errorf("%v", r)
				}
			}()
			state = listener.ExecuteLocalTransaction(msg, arg)
		}()
	case FlushDiskTimeout, FlushSlaveTimeout, SlaveNotAvailable:
		state = RollbackMessage
	}

	// Java: an endTransaction failure is only a warning; it does not change the
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

	if fromTransactionCheck {
		// The check request carries whatever the broker knew about the half
		// message (compression and transaction bookkeeping included).
		header.CommitLogOffset = checkHeader.CommitLogOffset
		header.TranStateTableOffset = checkHeader.TranStateTableOffset
		header.TransactionID = checkHeader.TransactionID
		header.Bname = checkHeader.Bname
		header.Topic = checkHeader.Topic
		if msgExt != nil {
			uniq := ""
			if v, ok := msgExt.GetProperty(common.PropertyUniqKey); ok {
				uniq = v
			}
			if uniq == "" {
				uniq = msgExt.MsgID
			}
			header.MsgID = strPtr(uniq)
		}
	} else {
		// Java: id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
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
		// An empty transaction id is left ABSENT, not written as "": Java's
		// reflection writer skips null fields and Python passes None, so an
		// empty string on the wire would be a third behaviour nobody expects.
		header.TransactionID = strPtrIfSet(sendResult.TransactionID)
		header.Bname = strPtr(brokerName)
		header.Topic = strPtr(msg.Topic)
		header.MsgID = strPtr(sendResult.MsgID)
		// Java endTransaction:1541 also uses findBrokerAddressInPublish (master
		// only) and does NOT null-check before the oneway send. This port keeps
		// the explicit guard (cleaner than Java's NPE) but the address source
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

	request := remoting.CreateRequestCommand(remoting.ReqEndTransaction, header)
	if localException != nil {
		request.Remark = fmt.Sprintf("executeLocalTransactionBranch exception: %v", localException)
	}
	if brokerAddr == "" {
		return common.ClientError("no broker address for end transaction")
	}
	return inst.Remoting().InvokeOneway(brokerAddr, request)
}

// transactionFlag maps LocalTransactionState to Java MessageSysFlag's
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
// (CHECK_TRANSACTION_STATE = 39), Java ClientRemotingProcessor
// .checkTransactionState + DefaultMQProducerImpl.checkTransactionState.
//
// The broker sends 39 as a ONEWAY request (body = the whole encoded MessageExt),
// so the client must NOT reply. Instead a goroutine calls
// listener.CheckLocalTransaction and then reports the verdict back with
// END_TRANSACTION(fromTransactionCheck = true).
//
// The listener must not be run on the remoting read goroutine: it is user code
// and may block, which would stall every other response on that connection.
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
	p.mu.Lock()
	listener := p.transactionListener
	p.mu.Unlock()
	if listener == nil {
		common.LogWarnf("checkTransactionState: no transaction listener for group %s", p.producerGroup)
		return
	}
	go func() {
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
	}()
}

// ---------------------------------------------------------------- misc

// FetchPublishMessageQueues mirrors Java fetchPublishMessageQueues.
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
// leaves the header field unset (Java's Integer.parseInt would throw, but a
// malformed property is not worth failing an otherwise valid send over).
func parseInt32(raw string) (int32, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}
