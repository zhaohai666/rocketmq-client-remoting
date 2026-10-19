package client

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------- offsets ----------------

// MaxOffset sends GET_MAX_OFFSET(30) (master only).
func (a *DefaultMQAdminExt) MaxOffset(mq common.MessageQueue) (int64, error) {
	instance, err := a.requireClient()
	if err != nil {
		return 0, err
	}
	return instance.GetMaxOffset(mq, a.timeout(0))
}

// MinOffset sends GET_MIN_OFFSET(31) (master only).
func (a *DefaultMQAdminExt) MinOffset(mq common.MessageQueue) (int64, error) {
	instance, err := a.requireClient()
	if err != nil {
		return 0, err
	}
	return instance.GetMinOffset(mq, a.timeout(0))
}

func (a *DefaultMQAdminExt) SearchOffset(mq common.MessageQueue, timestamp int64) (int64, error) {
	return a.searchOffsetWithBoundary(mq, timestamp, remoting.BoundaryLower)
}

func (a *DefaultMQAdminExt) SearchLowerBoundaryOffset(mq common.MessageQueue, timestamp int64) (int64, error) {
	return a.searchOffsetWithBoundary(mq, timestamp, remoting.BoundaryLower)
}

// It differs from LOWER only when several messages share a store time, or the
// timestamp falls in a gap or past the end of the queue: past the tail, UPPER
// returns the last message's own offset while LOWER returns the next one
// (maxOffset).
func (a *DefaultMQAdminExt) SearchUpperBoundaryOffset(mq common.MessageQueue, timestamp int64) (int64, error) {
	return a.searchOffsetWithBoundary(mq, timestamp, remoting.BoundaryUpper)
}

func (a *DefaultMQAdminExt) searchOffsetWithBoundary(mq common.MessageQueue, timestamp int64, boundary remoting.BoundaryType) (int64, error) {
	instance, err := a.requireClient()
	if err != nil {
		return 0, err
	}
	addr, err := instance.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	header := &remoting.SearchOffsetRequestHeader{
		Topic:        remoting.StrPtr(mq.Topic),
		QueueID:      remoting.I32Ptr(mq.QueueID),
		Timestamp:    remoting.I64Ptr(timestamp),
		BoundaryType: &boundary,
	}
	response, err := instance.invokeSync(a.brokerVIPAddr(addr),
		remoting.CreateRequestCommand(remoting.ReqSearchOffsetByTimestamp, header), a.timeout(0))
	if err != nil {
		return 0, err
	}
	if err := instance.checkResponse(response); err != nil {
		return 0, err
	}
	var respHeader remoting.SearchOffsetResponseHeader
	respHeader.FromExtFields(response.ExtFields())
	if respHeader.Offset == nil {
		return 0, nil
	}
	return *respHeader.Offset, nil
}

// EarliestMsgStoreTime sends GET_EARLIEST_MSG_STORETIME(32).
//
// Same address discipline as max/min/search: master only, one route refresh,
// then "The broker[X] not exist".
func (a *DefaultMQAdminExt) EarliestMsgStoreTime(mq common.MessageQueue) (int64, error) {
	instance, err := a.requireClient()
	if err != nil {
		return 0, err
	}
	addr, err := instance.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	response, err := instance.invokeSync(a.brokerVIPAddr(addr), adminRequest(remoting.ReqGetEarliestMsgStoretime,
		adminExt("topic", mq.Topic, "queueId", mq.QueueID, "brokerName", mq.BrokerName)), a.timeout(0))
	if err != nil {
		return 0, err
	}
	if err := instance.checkResponse(response); err != nil {
		return 0, err
	}
	return extInt64(response, "timestamp"), nil
}

// ExamineConsumerOffset reads one queue's committed offset. The second result
// is false when the broker has no offset for that queue (QUERY_NOT_FOUND).
func (a *DefaultMQAdminExt) ExamineConsumerOffset(consumerGroup string, mq common.MessageQueue) (int64, bool, error) {
	instance, err := a.requireClient()
	if err != nil {
		return 0, false, err
	}
	return instance.QueryConsumerOffset(consumerGroup, mq, a.timeout(0), "", false)
}

// UpdateConsumerOffset writes one queue's committed offset.
func (a *DefaultMQAdminExt) UpdateConsumerOffset(consumerGroup string, mq common.MessageQueue, offset int64) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.UpdateConsumerOffset(consumerGroup, mq, offset, a.timeout(0), "")
}

// UpdateConsumerOffsetToBroker writes an offset to an explicit broker.
func (a *DefaultMQAdminExt) UpdateConsumerOffsetToBroker(brokerAddr, consumerGroup string, mq common.MessageQueue, offset int64) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	return instance.UpdateConsumerOffset(consumerGroup, mq, offset, a.timeout(0), brokerAddr)
}

// ---------------- reset offset ----------------

// resetOffsetRouteTopic mirrors the LMQ / timer-wheel special case: those
// topics are not routable by name, so the CLUSTER name is used for the route
// lookup instead.
func resetOffsetRouteTopic(topic, clusterName string) string {
	if topic == "" || clusterName == "" {
		return topic
	}
	if strings.HasPrefix(topic, "%LMQ%") || topic == common.SystemTopicPrefix+"wheel_timer" {
		return clusterName
	}
	return topic
}

// ResetOffsetByTimestamp sends INVOKE_BROKER_TO_RESET_OFFSET(222) to every
// broker of the topic route; the broker computes the new offsets, syncs the
// online consumers (220) and updates its offset table. The returned map is the
// union of every broker's answer.
//
// This deliberately does NOT fall back to the "search each queue, then
// updateConsumerOffset" shape — that neither syncs online consumers nor does
// the broker-side consistency check.
func (a *DefaultMQAdminExt) ResetOffsetByTimestamp(topic, group string, timestamp int64,
	isForce bool, clusterName string, isCpp bool) (map[common.MessageQueue]int64, error) {

	route, err := a.ExamineTopicRoute(resetOffsetRouteTopic(topic, clusterName))
	if err != nil {
		return nil, err
	}
	all := map[common.MessageQueue]int64{}
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		part, err := a.invokeBrokerResetOffset(addr, topic, group, timestamp, isForce, isCpp, nil, nil)
		if err != nil {
			return nil, err
		}
		for q, off := range part {
			all[q] = off
		}
	}
	if len(all) == 0 {
		return nil, common.ClientError("reset offset failed, no broker returned offset table")
	}
	return all, nil
}

// invokeBrokerResetOffset issues one 222 request and returns the queue table
// the broker actually reset.
//
//	{"force":"true", timestamp:-1}   -> target 3  (= consumerOffset)
//	{"isForce":"true", timestamp:-1} -> target 10 (= maxOffset)
func (a *DefaultMQAdminExt) invokeBrokerResetOffset(brokerAddr, topic, group string,
	timestamp int64, isForce, isCpp bool, queueID *int32, offset *int64) (map[common.MessageQueue]int64, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	offsetValue := int64(-1)
	if offset != nil {
		offsetValue = *offset
	}
	ext := adminExt(
		"topic", topic,
		"group", group,
		"timestamp", timestamp,
		"isForce", isForce,
		"offset", offsetValue,
	)
	if queueID != nil {
		ext.Put("queueId", adminExtValue(*queueID))
	}
	request := adminRequest(remoting.ReqInvokeBrokerToResetOffset, ext)
	if isCpp {
		request.Language = remoting.LangCPP
	}
	response, err := instance.invokeSync(a.brokerVIPAddr(brokerAddr), request, a.timeout(0))
	if err != nil {
		return nil, err
	}
	if response.Code != remoting.RespSuccess {
		remark := response.Remark
		if remark == "" {
			remark = "reset offset failed"
		}
		return nil, common.ClientErrorCode(response.Code, remark)
	}
	if len(response.Body) == 0 {
		return map[common.MessageQueue]int64{}, nil
	}
	body, err := remoting.DecodeResetOffsetBody(response.Body)
	if err != nil {
		return nil, err
	}
	out := map[common.MessageQueue]int64{}
	for _, e := range body.OffsetTable {
		out[e.Queue] = e.Offset
	}
	return out, nil
}

//  1. updateConsumerOffset(15) rewrites offsetTable to the target;
//  2. the 222 with `queueId` + `offset` goes through
//     AdminBrokerProcessor#resetOffsetInner, which validates the target against
//     [min, max+1] (out of range -> SYSTEM_ERROR "Target offset N not in
//     consume queue range [min-max]"), then
//     ConsumerOffsetManager#assignResetOffset — which writes BOTH
//     resetOffsetTable (one-shot, taken by the next pull via
//     queryThenEraseResetOffset) and offsetTable, and clears the queue's POP
//     in-flight count.
func (a *DefaultMQAdminExt) ResetOffsetByQueueID(brokerAddr, consumerGroup, topic string,
	queueID int32, resetOffset int64) (map[common.MessageQueue]int64, error) {

	err := a.UpdateConsumerOffsetToBroker(brokerAddr, consumerGroup,
		common.NewMessageQueue(topic, "", queueID), resetOffset)
	if err != nil {
		return nil, err
	}
	return a.invokeBrokerResetOffset(brokerAddr, topic, consumerGroup, 0, false, false,
		&queueID, &resetOffset)
}

func (a *DefaultMQAdminExt) ResetOffsetNew(consumerGroup, topic string, timestamp int64) error {
	_, err := a.ResetOffsetByTimestamp(topic, consumerGroup, timestamp, true, "", false)
	if err == nil {
		return nil
	}
	if code, ok := responseCodeOf(err); ok && code == remoting.RespConsumerNotOnline {
		_, err = a.ResetOffsetByTimestampOld(consumerGroup, topic, timestamp, true)
		return err
	}
	return err
}

func (a *DefaultMQAdminExt) ResetOffsetByTimestampOld(consumerGroup, topic string, timestamp int64,
	force bool) (map[common.MessageQueue]int64, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	route, err := a.ExamineTopicRoute(topic)
	if err != nil {
		return nil, err
	}
	result := map[common.MessageQueue]int64{}
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		for _, qd := range route.QueueDatas {
			if qd.BrokerName != bd.BrokerName {
				continue
			}
			for queueID := int32(0); queueID < qd.ReadQueueNums; queueID++ {
				mq := common.NewMessageQueue(topic, bd.BrokerName, queueID)
				consumerOffset, _, err := instance.QueryConsumerOffset(consumerGroup, mq, a.timeout(0), addr, false)
				if err != nil {
					consumerOffset = 0
				}
				var resetOffset int64
				if timestamp == -1 {
					resetOffset, err = instance.GetMaxOffset(mq, a.timeout(0))
				} else {
					resetOffset, err = a.searchOffsetWithBoundary(mq, timestamp, remoting.BoundaryLower)
				}
				if err != nil {
					return nil, err
				}
				if force || resetOffset <= consumerOffset {
					if err := instance.UpdateConsumerOffset(consumerGroup, mq, resetOffset, a.timeout(0), addr); err != nil {
						return nil, err
					}
					result[mq] = resetOffset
				}
			}
		}
	}
	return result, nil
}

// ---------------- message query ----------------

// QueryMessage queries the KEYS index across every broker of the topic.
func (a *DefaultMQAdminExt) QueryMessage(topic, key string, maxNum int32, begin, end int64) ([]*common.MessageExt, error) {
	return a.queryMessageAllBrokers(topic, key, maxNum, begin, end, common.IndexKeyType, false)
}

// QueryMessageByKey queries the KEYS index over the last hour.
func (a *DefaultMQAdminExt) QueryMessageByKey(topic, key string, maxNum int32) ([]*common.MessageExt, error) {
	return a.queryMessageAllBrokers(topic, key, maxNum, 0, time.Now().UnixMilli()+3600_000,
		common.IndexKeyType, false)
}

// QueryMessageByUniqKey queries by UNIQ_KEY.
//
// Note the broker-side uniqKey index is only supported by the RocksDB index
// implementation (IndexRocksDBStore); with the default file index this can
// legitimately return nothing. That is a broker-configuration difference, not
// a client bug.
func (a *DefaultMQAdminExt) QueryMessageByUniqKey(topic, uniqKey string) (*common.MessageExt, error) {
	messages, err := a.queryMessageAllBrokers(topic, uniqKey, 32, 0,
		time.Now().UnixMilli()+3600_000, common.IndexUniqueType, true)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, nil
	}
	return messages[0], nil
}

func (a *DefaultMQAdminExt) queryMessageAllBrokers(topic, key string, maxNum int32,
	beginTimestamp, endTimestamp int64, indexType string, uniqKey bool) ([]*common.MessageExt, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	route := instance.GetTopicRouteData(topic)
	if route == nil {
		return []*common.MessageExt{}, nil
	}
	var messages []*common.MessageExt
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		body, err := a.queryMessageOneBroker(addr, topic, key, maxNum,
			beginTimestamp, endTimestamp, indexType, uniqKey)
		if err != nil {
			common.LogDebugf("queryMessage on %s failed: %v", addr, err)
			continue
		}
		if len(body) == 0 {
			continue
		}
		for _, m := range common.DecodeMessages(body) {
			m.BrokerName = bd.BrokerName
			if uniqKey {
				if m.MsgID == key {
					messages = append(messages, m)
				}
				continue
			}
			keys, ok := m.GetKeys()
			if !ok || keys == "" {
				continue
			}
			for _, k := range strings.Split(keys, common.KeySeparator) {
				if k == key && m.Topic == topic {
					messages = append(messages, m)
					break
				}
			}
		}
	}
	sortByQueueOffset(messages)
	if maxNum > 0 && len(messages) > int(maxNum) {
		messages = messages[:maxNum]
	}
	return messages, nil
}

// queryMessageOneBroker sends QUERY_MESSAGE(12) to one broker and returns the
// raw body (nil when the broker answered QUERY_NOT_FOUND).
func (a *DefaultMQAdminExt) queryMessageOneBroker(addr, topic, key string, maxNum int32,
	beginTimestamp, endTimestamp int64, indexType string, uniqKey bool) ([]byte, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	ext := adminExt(
		"topic", topic,
		"key", key,
		"maxNum", maxNum,
		"beginTimestamp", beginTimestamp,
		"endTimestamp", endTimestamp,
		"indexType", indexType,
	)
	request := adminRequest(remoting.ReqQueryMessage, ext)
	if uniqKey {
		// Forces the broker down the uniqKey index path (and overrides maxNum
		// with the default query count).
		request.AddExtField(common.UniqueMsgQueryFlag, "true")
	}
	response, err := instance.invokeSync(a.brokerVIPAddr(addr), request, a.timeout(0))
	if err != nil {
		return nil, err
	}
	if response.Code == remoting.RespQueryNotFound {
		return nil, nil
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (a *DefaultMQAdminExt) ViewMessage(topic, msgID string) (*common.MessageExt, error) {
	msg, err := a.viewMessageByID(topic, msgID)
	if err == nil && msg != nil {
		return msg, nil
	}
	found, qErr := a.QueryMessageByUniqKey(topic, msgID)
	if qErr != nil {
		return nil, qErr
	}
	if found != nil {
		return found, nil
	}
	return nil, common.ClientError(fmt.Sprintf(
		"viewMessage failed: neither offset msgId nor uniq key matched message %s of %s",
		msgID, topic))
}

// viewMessageByID performs the VIEW_MESSAGE_BY_ID(33) attempt.
func (a *DefaultMQAdminExt) viewMessageByID(topic, msgID string) (*common.MessageExt, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	ip, port, offset, err := common.DecodeMessageID(msgID)
	if err != nil {
		return nil, err
	}
	if port == 0 || port > 65535 {
		return nil, common.ClientError(fmt.Sprintf("not a valid offset msgId: %s", msgID))
	}
	// The address is the store host:port the broker stamped into the offset
	// msgId — a broker address, so the VIP-channel rewrite applies to it like to
	// any other query request.
	addr := fmt.Sprintf("%s:%d", ip, port)
	response, err := instance.invokeSync(a.brokerVIPAddr(addr), adminRequest(remoting.ReqViewMessageByID,
		adminExt("topic", topic, "offset", offset)), a.timeout(0))
	if err != nil {
		return nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.BrokerError(remoting.RespNoMessage,
			fmt.Sprintf("message not found: %s", msgID))
	}
	return common.DecodeMessage(response.Body)
}

// QueryConsumeQueue sends QUERY_CONSUME_QUEUE(321).
func (a *DefaultMQAdminExt) QueryConsumeQueue(brokerAddr, topic string, queueID int32,
	index int64, count int32, consumerGroup string) (*remoting.QueryConsumeQueueResponseBody, error) {

	response, err := a.invokeBroker(brokerAddr, remoting.ReqQueryConsumeQueue, adminExt(
		"topic", topic,
		"queueId", queueID,
		"index", index,
		"count", count,
		"consumerGroup", consumerGroup,
	), nil, 0)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeQueryConsumeQueueResponseBody(response.Body)
}

func (a *DefaultMQAdminExt) ConsumeMessageDirectly(consumerGroup, clientID, topic, msgID string) (*remoting.ConsumeMessageDirectlyResult, error) {
	msg, err := a.ViewMessage(topic, msgID)
	if err != nil {
		return nil, err
	}
	outMsgID := msgID
	if _, uniq := msg.GetProperty(common.PropertyUniqKey); uniq && msg.OffsetMsgID != "" {
		outMsgID = msg.OffsetMsgID
	}
	return a.consumeMessageDirectlyTo(msg.StoreHostString(), consumerGroup, clientID, topic, outMsgID)
}

func (a *DefaultMQAdminExt) consumeMessageDirectlyTo(brokerAddr, consumerGroup, clientID, topic, msgID string) (*remoting.ConsumeMessageDirectlyResult, error) {
	response, err := a.invokeBroker(brokerAddr, remoting.ReqConsumeMessageDirectly, adminExt(
		"consumerGroup", consumerGroup,
		"clientId", clientID,
		"msgId", msgID,
		"topic", topic,
	), nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientError(fmt.Sprintf("no consume result for client %s", clientID))
	}
	return remoting.DecodeConsumeMessageDirectlyResult(response.Body)
}
