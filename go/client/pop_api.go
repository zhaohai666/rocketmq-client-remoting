// POP path, wire side (Java MQClientAPIImpl.processPopResponse + PullAPIWrapper
// .popAsync + ExtraInfoUtil).
//
// POP is not "another pull". A pull names a queue and an offset and the broker
// answers with messages; a POP asks for a batch from one queue, and the broker
// answers with messages PLUS a checkpoint (POP_CK) describing the pop session
// (popTime, invisibleTime, reviveQid) and per-queue offset tables. The client
// must:
//
//  1. rebuild that checkpoint per message, ending in the message's own queue
//     offset (segment 7) — that value, not the batch start, is what ACK sends;
//  2. ACK with offset = segment 7;
//  3. never commit a consumer offset: POP leaves the offset to the broker's
//     revive logic. The consumer offset table is NOT advanced by a POP at all.
//
// The two POP_CK construction paths here are both real. When the broker sent
// startOffsetInfo/msgOffsetInfo the checkpoint is derived from those tables
// (the batch-start offset comes from the table, not from the message). When it
// did not, the checkpoint is built from the message's own queue offset. Java
// checks `startOffsetInfo == null` to choose; so does this file.
package client

import (
	"fmt"
	"sort"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// PopStatus mirrors Java org.apache.rocketmq.client.consumer.PopStatus.
type PopStatus int

const (
	// PopFound: the broker returned messages.
	PopFound PopStatus = iota
	// PopNoNewMsg: nothing to hand out.
	PopNoNewMsg
	// PopPollingFull: the broker's polling pool is full — back off, do not
	// retry immediately (Java treats this as the "later" branch).
	PopPollingFull
	// PopPollingNotFound: the long poll expired with no message.
	PopPollingNotFound
)

func (s PopStatus) String() string {
	switch s {
	case PopFound:
		return "FOUND"
	case PopNoNewMsg:
		return "NO_NEW_MSG"
	case PopPollingFull:
		return "POLLING_FULL"
	case PopPollingNotFound:
		return "POLLING_NOT_FOUND"
	default:
		return fmt.Sprintf("PopStatus(%d)", int(s))
	}
}

// PopResult is one POP response (Java PopResult, restricted to what the classic
// push consumer reads).
type PopResult struct {
	PopStatus    PopStatus
	MsgFoundList []*common.MessageExt
	RestNum      int64
	// PopTime / InvisibleTime come from the response header and are what the
	// per-message checkpoint echoes.
	PopTime       int64
	InvisibleTime int64
}

// ---------------------------------------------------------------- instance API

// PopMessage sends POP_MESSAGE(200050) to one broker and turns the reply into a
// PopResult with a POP_CK stamped on every message.
//
// brokerName is the LOGICAL name the checkpoint must carry (segment 5) and must
// be the same name the ACK later addresses — sending the physical broker name
// here makes every ACK unresolvable.
//
// namespace is the client-side namespace to strip from the topic handed to the
// listener ("" when unset).
func (i *Instance) PopMessage(brokerName, addr string, header *remoting.PopMessageRequestHeader, namespace string, timeoutMillis int64) (*PopResult, error) {
	request := remoting.CreateRequestCommand(remoting.ReqPopMessage, header)
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return i.processPopResponse(brokerName, response, header, namespace)
}

// AckMessage sends ACK_MESSAGE(200051) synchronously. `offset` must be the
// checkpoint's segment 7 (the message's own queue offset).
func (i *Instance) AckMessage(addr string, header *remoting.AckMessageRequestHeader, timeoutMillis int64) error {
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqAckMessage, header), timeoutMillis)
	if err != nil {
		return err
	}
	if response.Code != remoting.RespSuccess {
		// Java maps every non-SUCCESS code to AckStatus.NO_EXIST — the broker
		// could not find the checkpoint, which is a warning, not a hard error.
		return common.BrokerError(response.Code, response.Remark)
	}
	return nil
}

// BatchAckMessage sends BATCH_ACK_MESSAGE(200151) with a pre-built body.
//
// NOTE (Java fidelity): the CLASSIC Java client never calls this. Its POP path
// acks one message at a time through DefaultMQPushConsumerImpl#ackAsync ->
// MQClientAPIImpl#ackMessageAsync, and BATCH_ACK_MESSAGE(200151) appears nowhere
// in client/src/main/java. It is exported here because the wire capability is
// real (the broker implements it and the next-gen/proxy clients use it) and the
// live verifier exercises it directly. Do NOT "finish" the POP consumer by
// routing its acks through this — that would be inventing client behaviour.
func (i *Instance) BatchAckMessage(addr string, body *remoting.BatchAckMessageRequestBody, timeoutMillis int64) error {
	// Note: no custom header — Java passes null and rides the body only.
	request := remoting.CreateRequestCommand(remoting.ReqBatchAckMessage, nil)
	request.SetBody(body.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response.Code != remoting.RespSuccess {
		return common.BrokerError(response.Code, response.Remark)
	}
	return nil
}

// ChangeInvisibleTime sends CHANGE_MESSAGE_INVISIBLETIME(200053) and returns the
// NEW popTime / invisibleTime the broker assigned.
//
// The returned values are not cosmetic: Java rebuilds the checkpoint from them
// so a later ACK still matches the (now longer) invisibility window.
func (i *Instance) ChangeInvisibleTime(addr string, header *remoting.ChangeInvisibleTimeRequestHeader, timeoutMillis int64) (*remoting.ChangeInvisibleTimeResponseHeader, error) {
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqChangeMessageInvisibleTime, header), timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code != remoting.RespSuccess {
		return nil, common.BrokerError(response.Code, response.Remark)
	}
	respHeader := &remoting.ChangeInvisibleTimeResponseHeader{}
	respHeader.FromExtFields(response.ExtFields())
	return respHeader, nil
}

// SetMessageRequestMode sends SET_MESSAGE_REQUEST_MODE(401) — the broker-side
// switch that decides whether a (group, topic) pair is served in POP or PULL
// mode.
func (i *Instance) SetMessageRequestMode(addr, topic, consumerGroup, mode string, popShareQueueNum int32, timeoutMillis int64) error {
	body := &remoting.SetMessageRequestModeRequestBody{
		Topic:            topic,
		ConsumerGroup:    consumerGroup,
		Mode:             mode,
		PopShareQueueNum: popShareQueueNum,
	}
	request := remoting.CreateRequestCommand(remoting.ReqSetMessageRequestMode, nil)
	request.SetBody(body.Encode())
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response.Code != remoting.RespSuccess {
		return common.ClientErrorCode(response.Code, response.Remark)
	}
	return nil
}

// ---------------------------------------------------------------- response

func (i *Instance) processPopResponse(brokerName string, response *remoting.RemotingCommand,
	header *remoting.PopMessageRequestHeader, namespace string) (*PopResult, error) {

	result := &PopResult{PopStatus: PopNoNewMsg}
	switch response.Code {
	case remoting.RespSuccess:
		result.PopStatus = PopFound
		if len(response.Body) > 0 {
			result.MsgFoundList = common.DecodeMessages(response.Body)
		}
	case remoting.RespPollingFull:
		result.PopStatus = PopPollingFull
	case remoting.RespPollingTimeout:
		result.PopStatus = PopPollingNotFound
	case remoting.RespPullNotFound:
		result.PopStatus = PopPollingNotFound
	default:
		return nil, common.BrokerError(response.Code, response.Remark)
	}

	respHeader := &remoting.PopMessageResponseHeader{}
	respHeader.FromExtFields(response.ExtFields())
	result.RestNum = i64Or(respHeader.RestNum, 0)
	if result.PopStatus != PopFound {
		// Java returns early: an empty answer has no checkpoint tables to parse,
		// and the header's popTime is meaningless for a message that does not
		// exist.
		return result, nil
	}
	result.PopTime = i64Or(respHeader.PopTime, 0)
	result.InvisibleTime = i64Or(respHeader.InvisibleTime, 0)

	startOffsetInfo, err := common.ParseStartOffsetInfo(derefStr(respHeader.StartOffsetInfo))
	if err != nil {
		return nil, err
	}
	msgOffsetInfo, err := common.ParseMsgOffsetInfo(derefStr(respHeader.MsgOffsetInfo))
	if err != nil {
		return nil, err
	}
	orderCountInfo, err := common.ParseOrderCountInfo(derefStr(respHeader.OrderCountInfo))
	if err != nil {
		return nil, err
	}

	reviveQid := int(i32Or(respHeader.ReviveQid, 0))
	topic := derefStr(header.Topic)
	sortMap := buildQueueOffsetSortedMap(result.MsgFoundList)
	built := map[string]string{}

	for _, msg := range result.MsgFoundList {
		if startOffsetInfo == nil {
			// No tables: the checkpoint is built from this message's own queue
			// offset. Java caches it per (topic,queueId) and appends the
			// message offset as segment 7.
			key := msg.Topic + i32Text(msg.QueueID)
			checkpoint, ok := built[key]
			if !ok {
				checkpoint = common.BuildExtraInfo(msg.QueueOffset, result.PopTime, result.InvisibleTime,
					reviveQid, msg.Topic, brokerName, int(msg.QueueID))
				built[key] = checkpoint
			}
			msg.PutProperty(common.PropertyPopCk, checkpoint+common.KeySeparator+i64Text(msg.QueueOffset))
		} else if propertyOf(msg, common.PropertyPopCk) == "" {
			// Java guards this whole branch with `getProperty(POP_CK) == null`:
			// when the broker already stamped a checkpoint, KEEP IT.
			//
			// That guard is load-bearing on the default retry path. With
			// brokerConfig.popResponseReturnActualRetryTopic=false the broker
			// builds the checkpoint from the PHYSICAL retry topic, so segment 4
			// (the retry marker) is "1", stamps it with putIfAbsent, and only
			// THEN rewrites msg.Topic to the business topic
			// (PopMessageProcessor:849-853). By the time the client sees the
			// message the topic has already been rewritten, so rebuilding from
			// the message would resolve the marker to "0" and derive the ACK
			// topic as the business topic — where the broker holds no such
			// checkpoint. The ACK then becomes a silent no-op whose only
			// symptom is "the message keeps coming back after popInvisibleTime".
			//
			// The lookup key is the message's OWN topic as decoded, which is
			// still the physical topic at this point (the request topic is
			// stamped on at the end of the loop). For a retried message with
			// popResponseReturnActualRetryTopic=true that is
			// `%RETRY%<group>_<topic>`, so the marker resolves to "1" and the
			// entry lines up with the one the broker emitted.
			queueIDKey := common.GetStartOffsetInfoMapKey(msg.Topic, int64(msg.QueueID))
			queueOffsetKey := common.GetQueueOffsetMapKey(msg.Topic, int64(msg.QueueID), msg.QueueOffset)
			ckOffset, okCk := startOffsetInfo[queueIDKey]
			msgOffsets, okMsg := msgOffsetInfo[queueIDKey]
			if !okCk || !okMsg {
				// The broker's tables do not cover this message. Java lets the
				// index lookup miss and keeps the previous (armoured) value, so
				// the message ends up with whatever POP_CK it arrived with.
				continue
			}
			index := indexOfI64(sortMap[queueIDKey], msg.QueueOffset)
			if index < 0 || index >= len(msgOffsets) {
				continue
			}
			msgQueueOffset := msgOffsets[index]
			msg.PutProperty(common.PropertyPopCk, common.BuildExtraInfoWithMsgQueueOffset(
				ckOffset, result.PopTime, result.InvisibleTime, reviveQid,
				msg.Topic, brokerName, int(msg.QueueID), msgQueueOffset))

			if header.IsOrder() && orderCountInfo != nil {
				// An orderly POP hides the reconsume count in orderCountInfo;
				// the key is the (queueId,queueOffset) form first, then plain
				// queueId.
				count, ok := orderCountInfo[queueOffsetKey]
				if !ok {
					count = orderCountInfo[queueIDKey]
				}
				if count > 0 {
					msg.ReconsumeTimes = int32(count)
				}
			}
		}
		// 1ST_POP_TIME is the FIRST pop of this message across all redeliveries;
		// only set it when absent, never overwrite.
		if _, exists := msg.GetProperty(common.PropertyFirstPopTime); !exists {
			msg.PutProperty(common.PropertyFirstPopTime, i64Text(result.PopTime))
		}
		msg.BrokerName = brokerName
		// The topic handed to the listener is the one the caller subscribed to,
		// with the namespace stripped — NOT the physical pop topic.
		msg.Topic = common.WithoutNamespace(topic, namespace)
	}
	return result, nil
}

// buildQueueOffsetSortedMap is Java MQClientAPIImpl#buildQueueOffsetSortedMap:
// one list of queue offsets per "<retry>@<queueId>", in the order the messages
// arrived. It is the index space that msgOffsetInfo is parallel to, which is why
// the lookup is indexOf rather than a search by value.
//
// The name says "sorted" but Java never sorts; the broker's msgOffsetInfo is
// built against the same arrival order.
//
// Java builds the key with the checkpoint-carrying overload, which prefers the
// marker inside POP_CK when the broker already stamped one. On the classic POP
// path the broker does not, so this falls back to the topic's own marker — and
// the lookup site uses the plain two-argument form so the two agree.
func buildQueueOffsetSortedMap(msgs []*common.MessageExt) map[string][]int64 {
	out := make(map[string][]int64, len(msgs))
	for _, msg := range msgs {
		key := common.GetStartOffsetInfoMapKeyWithTopic(msg.Topic, propertyOf(msg, common.PropertyPopCk), int64(msg.QueueID))
		out[key] = append(out[key], msg.QueueOffset)
	}
	return out
}

// ---------------------------------------------------------------- delay levels

// popDelayLevel is Java DefaultMQPushConsumerImpl.popDelayLevel, in SECONDS.
// Used only by changePopInvisibleTime (retry back-off), never as a broker
// delay-level.
var popDelayLevel = []int64{
	10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
}

// popDelayLevelSeconds picks the table entry for a delay level, clamping to the
// last entry — Java does exactly this instead of erroring.
func popDelayLevelSeconds(level int) int64 {
	if level < 0 {
		level = 0
	}
	if level >= len(popDelayLevel) {
		return popDelayLevel[len(popDelayLevel)-1]
	}
	return popDelayLevel[level]
}

// popDelayLevelForElapsed is the search half of Java's checkNeedAckOrDelay,
// reproduced exactly: scan DOWNWARD from the longest notch for the first delay
// that has ALREADY elapsed, then return the notch above it (the one to wait
// for now).
//
// Two sentinels are part of the contract, not accidents:
//
//   - Below the shortest notch (elapsed < 10s) nothing has elapsed and the loop
//     falls off the end with -1. Java then indexes the table with -1 and throws
//     ArrayIndexOutOfBoundsException; checkNeedAckOrDelay clamps this to 0,
//     i.e. wait the 10s notch.
//   - At or above the longest notch the result is len(table), which
//     popDelayLevelSeconds clamps to the last entry.
func popDelayLevelForElapsed(elapsedMillis int64) int {
	level := len(popDelayLevel) - 1
	for ; level >= 0; level-- {
		if elapsedMillis >= popDelayLevel[level]*1000 {
			level++
			break
		}
	}
	return level
}

// ---------------------------------------------------------------- helpers

// popCommitAddress resolves the broker address an ACK/CHANGE_INVISIBLETIME
// belongs to. Java finds it by the LOGICAL broker name from checkpoint segment
// 5 against the master, refreshing the route once.
//
// Deliberate divergence: Java's ackAsync/changePopInvisibleTimeAsync first map a
// checkpoint broker name beginning with "__syslo__" (MixAll
// .LOGICAL_QUEUE_MOCK_BROKER_PREFIX) through MQClientInstance
// #getBrokerNameFromMessageQueue, i.e. through the topicEndPointsTable built
// from TopicRouteData.topicQueueMapping. This port decodes
// topicQueueMappingByBroker as an opaque MappingEntry list and keeps no
// topicEndPointsTable, so there is no address to map to and the branch is not
// taken. The static-topic / logical-queue routing feature is therefore a known
// P3 gap, not a silent fallback: the header still carries the LOGICAL name Java
// would send.
func (c *DefaultMQPushConsumer) popAckAddress(brokerName, topic string) (string, error) {
	inst := c.instance
	if inst == nil {
		return "", common.ClientError("consumer not started")
	}
	addr, _, found := inst.FindBrokerAddressInSubscribe(brokerName, int64(common.MasterID), true)
	if !found {
		if _, err := inst.UpdateTopicRouteInfoFromNameServer(topic, 5000, false); err != nil {
			return "", err
		}
		addr, _, found = inst.FindBrokerAddressInSubscribe(brokerName, int64(common.MasterID), true)
	}
	if !found {
		return "", common.ClientError(fmt.Sprintf("The broker[%s] master node does not exist", brokerName))
	}
	return addr, nil
}

func propertyOf(msg *common.MessageExt, name string) string {
	if msg.Properties == nil {
		return ""
	}
	v, _ := msg.GetProperty(name)
	return v
}

func derefStr(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func indexOfI64(list []int64, value int64) int {
	for i, v := range list {
		if v == value {
			return i
		}
	}
	return -1
}

// sortedI64 is used by the verifiers/diagnostics only.
func sortedI64(list []int64) []int64 {
	out := append([]int64(nil), list...)
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
