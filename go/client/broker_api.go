// Broker RPCs the consumer needs but the producer never did: rebalance's
// consumer-list query, the orderly lock/unlock pair, the CONSUME_FROM_TIMESTAMP
// offset search, and the offset queries used to resolve an initial position.
//
// The address discipline differs per call and is NOT interchangeable:
//
//   - getConsumerListByGroup: master preferred (any address for the broker
//     works in principle, but the master is the one the admin path uses);
//   - lockBatchMQ / unlockBatchMQ: MASTER ONLY, no route refresh on a miss.
//     Locking on a slave registers the lock in that slave's lock manager while
//     the master stays unaware, so orderly mutual exclusion silently stops
//     working;
//   - CONSUMER_SEND_MSG_BACK(36): MASTER ONLY (a slave does not accept it);
//   - offset queries (max/min/timestamp): master first, one route refresh,
//     then master again (MQAdminImpl口径).
package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// GetConsumerListByGroup sends GET_CONSUMER_LIST_BY_GROUP(38) to one broker and
// returns the group's online clientIds. Used by rebalance.
func (i *Instance) GetConsumerListByGroup(addr, group string, timeoutMillis int64) ([]string, error) {
	header := &remoting.GetConsumerListByGroupRequestHeader{ConsumerGroup: remoting.StrPtr(group)}
	request := remoting.CreateRequestCommand(remoting.ReqGetConsumerListByGroup, header)
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if err := i.checkResponse(response); err != nil {
		return nil, err
	}
	body, err := remoting.DecodeGetConsumerListByGroupResponseBody(response.Body)
	if err != nil {
		return nil, err
	}
	return body.ConsumerIDList, nil
}

// GetConsumerIDListByGroup is Java MQClientInstance#findConsumerIdList: ask one
// master of the topic's route. Every client heartbeats to every broker, so any
// one broker holds the COMPLETE list for the group.
//
// The second return value is "the question was answered": false means no route,
// a transport error, or a non-SUCCESS reply. Callers must then KEEP their
// current assignment — never fall back to "I own every queue", which makes
// co-instances duplicate each other.
func (i *Instance) GetConsumerIDListByGroup(topic, group string, timeoutMillis int64) ([]string, bool) {
	addr, err := i.AddrFor(topic, "")
	if err != nil {
		common.LogDebugf("getConsumerIdListByGroup: no broker for topic %s: %v", topic, err)
		return nil, false
	}
	ids, err := i.GetConsumerListByGroup(addr, group, timeoutMillis)
	if err != nil {
		common.LogDebugf("getConsumerIdListByGroup failed, %s %s: %v", addr, group, err)
		return nil, false
	}
	return ids, true
}

// LockBatchMQ sends LOCK_BATCH_MQ(41), grouped per broker, and returns the
// queues the brokers confirmed. A broker whose master address is missing is
// skipped entirely (see the file comment).
func (i *Instance) LockBatchMQ(group, clientID string, mqs []common.MessageQueue, timeoutMillis int64) []common.MessageQueue {
	return i.batchLock(group, clientID, mqs, timeoutMillis, true)
}

// UnlockBatchMQ sends UNLOCK_BATCH_MQ(42). Best effort: failures are logged.
func (i *Instance) UnlockBatchMQ(group, clientID string, mqs []common.MessageQueue, timeoutMillis int64) {
	i.batchLock(group, clientID, mqs, timeoutMillis, false)
}

func (i *Instance) batchLock(group, clientID string, mqs []common.MessageQueue, timeoutMillis int64, lock bool) []common.MessageQueue {
	byBroker := map[string][]common.MessageQueue{}
	order := []string{}
	for _, mq := range mqs {
		if _, ok := byBroker[mq.BrokerName]; !ok {
			order = append(order, mq.BrokerName)
		}
		byBroker[mq.BrokerName] = append(byBroker[mq.BrokerName], mq)
	}

	var locked []common.MessageQueue
	for _, brokerName := range order {
		// Master only, and deliberately NO route refresh: an unlocatable master
		// means "this round cannot lock", and the next round retries.
		addr, ok := i.FindBrokerAddressInPublish(brokerName)
		if !ok {
			continue
		}
		code := remoting.ReqUnlockBatchMQ
		var request *remoting.RemotingCommand
		if lock {
			code = remoting.ReqLockBatchMQ
			body := &remoting.LockBatchRequestBody{
				ConsumerGroup: remoting.StrPtr(group),
				ClientID:      remoting.StrPtr(clientID),
				MQSet:         byBroker[brokerName],
			}
			request = remoting.CreateRequestCommand(code, &remoting.LockBatchMqRequestHeader{
				ConsumerGroup: remoting.StrPtr(group),
				ClientID:      remoting.StrPtr(clientID),
			})
			request.SetBody(body.Encode())
		} else {
			body := &remoting.UnlockBatchRequestBody{
				ConsumerGroup: remoting.StrPtr(group),
				ClientID:      remoting.StrPtr(clientID),
				MQSet:         byBroker[brokerName],
			}
			request = remoting.CreateRequestCommand(code, &remoting.UnlockBatchMqRequestHeader{
				ConsumerGroup: remoting.StrPtr(group),
				ClientID:      remoting.StrPtr(clientID),
			})
			request.SetBody(body.Encode())
		}
		response, err := i.invokeSync(addr, request, timeoutMillis)
		if err != nil {
			common.LogWarnf("batch lock/unlock failed for broker %s: %v", brokerName, err)
			continue
		}
		if err := i.checkResponse(response); err != nil {
			common.LogWarnf("batch lock/unlock rejected by broker %s: %v", brokerName, err)
			continue
		}
		if !lock {
			continue
		}
		body, err := remoting.DecodeLockBatchResponseBody(response.Body)
		if err != nil {
			common.LogWarnf("batch lock reply from %s not decodable: %v", brokerName, err)
			continue
		}
		locked = append(locked, body.LockOKMQSet...)
	}
	return locked
}

// adminPublishAddr is Java MQAdminImpl's offset lookup: master-only publish
// address, one route refresh, master again, then "The broker[X] not exist".
// It never falls back to a slave — the management path is supposed to fail
// loudly while the master is down.
func (i *Instance) adminPublishAddr(mq common.MessageQueue) (string, error) {
	return i.PublishAddrFor(mq.BrokerName, mq.Topic)
}

// GetMaxOffset sends GET_MAX_OFFSET(30) for one queue.
func (i *Instance) GetMaxOffset(mq common.MessageQueue, timeoutMillis int64) (int64, error) {
	addr, err := i.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	header := &remoting.GetMaxOffsetRequestHeader{
		Topic:   remoting.StrPtr(mq.Topic),
		QueueID: remoting.I32Ptr(mq.QueueID),
	}
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqGetMaxOffset, header), timeoutMillis)
	if err != nil {
		return 0, err
	}
	if err := i.checkResponse(response); err != nil {
		return 0, err
	}
	var respHeader remoting.GetMaxOffsetResponseHeader
	respHeader.FromExtFields(response.ExtFields())
	return i64Or(respHeader.Offset, 0), nil
}

// GetMinOffset sends GET_MIN_OFFSET(31) for one queue.
func (i *Instance) GetMinOffset(mq common.MessageQueue, timeoutMillis int64) (int64, error) {
	addr, err := i.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	header := &remoting.GetMinOffsetRequestHeader{
		Topic:   remoting.StrPtr(mq.Topic),
		QueueID: remoting.I32Ptr(mq.QueueID),
	}
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqGetMinOffset, header), timeoutMillis)
	if err != nil {
		return 0, err
	}
	if err := i.checkResponse(response); err != nil {
		return 0, err
	}
	var respHeader remoting.GetMinOffsetResponseHeader
	respHeader.FromExtFields(response.ExtFields())
	return i64Or(respHeader.Offset, 0), nil
}

// SearchOffsetByTimestamp sends SEARCH_OFFSET_BY_TIMESTAMP(29) with
// boundaryType=LOWER (Java's MQAdminImpl.searchOffset uses LOWER, i.e. "the
// first message stored at or after the timestamp").
func (i *Instance) SearchOffsetByTimestamp(mq common.MessageQueue, timestamp int64, timeoutMillis int64) (int64, error) {
	addr, err := i.adminPublishAddr(mq)
	if err != nil {
		return 0, err
	}
	lower := remoting.BoundaryLower
	header := &remoting.SearchOffsetRequestHeader{
		Topic:        remoting.StrPtr(mq.Topic),
		QueueID:      remoting.I32Ptr(mq.QueueID),
		Timestamp:    remoting.I64Ptr(timestamp),
		BoundaryType: &lower,
	}
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqSearchOffsetByTimestamp, header), timeoutMillis)
	if err != nil {
		return 0, err
	}
	if err := i.checkResponse(response); err != nil {
		return 0, err
	}
	var respHeader remoting.SearchOffsetResponseHeader
	respHeader.FromExtFields(response.ExtFields())
	return i64Or(respHeader.Offset, 0), nil
}

// sendMessageBack is Java DefaultMQPushConsumerImpl#sendMessageBack: the
// cluster-side redelivery path (CONSUMER_SEND_MSG_BACK = 36).
//
// `offset` is the message's commitLogOffset, NOT its queueOffset — the broker
// looks the record up by physical offset.
func (i *Instance) sendMessageBack(group string, msg *common.MessageExt, delayLevel int32,
	maxReconsumeTimes int32, unitMode bool, timeoutMillis int64) error {

	addr, ok := i.FindBrokerAddressInPublish(msg.BrokerName)
	if !ok {
		return common.ClientError(fmt.Sprintf("Broker[%s] master node does not exist", msg.BrokerName))
	}
	header := &remoting.ConsumerSendMsgBackRequestHeader{
		Offset:            remoting.I64Ptr(msg.CommitLogOffset),
		Group:             remoting.StrPtr(group),
		DelayLevel:        remoting.I32Ptr(delayLevel),
		OriginMsgID:       remoting.StrPtr(msg.MsgID),
		OriginTopic:       remoting.StrPtr(msg.Topic),
		UnitMode:          remoting.BoolPtr(unitMode),
		MaxReconsumeTimes: remoting.I32Ptr(maxReconsumeTimes),
	}
	response, err := i.invokeSync(addr, remoting.CreateRequestCommand(remoting.ReqConsumerSendMsgBack, header), timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
}
