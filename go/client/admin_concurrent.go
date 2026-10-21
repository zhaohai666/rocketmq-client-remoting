// The concurrent admin fan-outs: one route lookup, then a goroutine per
// broker (or per consumer group), bounded by a latch-style wait — a task that
// finishes after the deadline is not cancelled, its result is simply not
// observed by this round. Every failure folds into an AdminToolResult so
// tooling gets a stable code instead of a thrown exception.
package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// AdminToolResult is the envelope of the concurrent admin fan-outs.
type AdminToolResult struct {
	Success  bool
	Code     int
	ErrorMsg string
	Data     any
}

// Result codes of the admin tool envelope.
const (
	AdminToolSuccess                = 200
	AdminToolRemotingError          = -1001
	AdminToolMQBrokerError          = -1002
	AdminToolMQClientError          = -1003
	AdminToolInterruptError         = -1004
	AdminToolTopicRouteInfoNotExist = -2001
	AdminToolConsumerNotOnline      = -2002
	AdminToolBroadcastConsumption   = -2003
)

// BrokerOperatorResult collects the per-broker outcome of the concurrent
// topic-delete / offset-reset fan-outs.
type BrokerOperatorResult struct {
	SuccessList []string
	FailureList []string
}

// adminToolFailure carries a pre-formed failure result through the handler.
type adminToolFailure struct{ result *AdminToolResult }

func (e *adminToolFailure) Error() string { return e.result.ErrorMsg }

func adminToolFailureResult(code int, message string) *AdminToolResult {
	return &AdminToolResult{Success: false, Code: code, ErrorMsg: message}
}

func adminToolFailureResultWithData(code int, message string, data any) *AdminToolResult {
	return &AdminToolResult{Success: false, Code: code, ErrorMsg: message, Data: data}
}

// adminToolExecute runs one fan-out handler and folds every failure into the
// envelope.
func adminToolExecute(handler func() (any, error)) *AdminToolResult {
	data, err := handler()
	if err == nil {
		return &AdminToolResult{Success: true, Code: AdminToolSuccess, ErrorMsg: "success", Data: data}
	}
	if failure, ok := err.(*adminToolFailure); ok {
		return failure.result
	}
	return adminToolResultFromError(err)
}

// adminToolResultFromError maps a raised error onto the envelope codes:
// transport failures report REMOTING_ERROR, a client error carrying the
// topic-not-exist code reports TOPIC_ROUTE_INFO_NOT_EXIST, other client
// errors report MQ_CLIENT_ERROR, and everything else — broker rejections
// included — reports MQ_BROKER_ERROR.
func adminToolResultFromError(err error) *AdminToolResult {
	transport := common.IsKind(err, common.KindConnect) ||
		common.IsKind(err, common.KindSendRequest) ||
		common.IsKind(err, common.KindTimeout) ||
		common.IsKind(err, common.KindRequestTimeout) ||
		common.IsKind(err, common.KindTooMuchRequest) ||
		common.IsKind(err, common.KindRemotingCommand)
	if transport {
		return adminToolFailureResult(AdminToolRemotingError, err.Error())
	}
	if common.IsKind(err, common.KindClient) {
		if code, ok := responseCodeOf(err); ok && code == remoting.RespTopicNotExist {
			return adminToolFailureResult(AdminToolTopicRouteInfoNotExist, err.Error())
		}
		return adminToolFailureResult(AdminToolMQClientError, err.Error())
	}
	return adminToolFailureResult(AdminToolMQBrokerError, err.Error())
}

// examineTopicRouteForFanout fetches the route straight from the nameserver
// and keeps the not-exist code on the raised error, so the envelope can tell
// a missing route apart from other client failures.
func (a *DefaultMQAdminExt) examineTopicRouteForFanout(topic string) (*TopicRouteData, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	route, err := instance.fetchTopicRouteFromNamesrv(topic, a.timeout(0), instance.NameServerAddrs())
	if err != nil {
		return nil, err
	}
	if route == nil {
		return nil, common.ClientErrorCode(remoting.RespTopicNotExist, fmt.Sprintf("topic[%s] not exist", topic))
	}
	return route, nil
}

// awaitFanout waits for the fan-out tasks at most timeoutMillis. Tasks that
// finish late are not cancelled; their writes just miss this round.
func awaitFanout(wg *sync.WaitGroup, timeoutMillis int64) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	if timeoutMillis <= 0 {
		<-done
		return
	}
	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// adminCodeIs reports whether err carries the given response code.
func adminCodeIs(err error, code int32) bool {
	c, ok := responseCodeOf(err)
	return ok && c == code
}

// ---------------- examineTopicStatsConcurrent ----------------

// ExamineTopicStatsConcurrent merges the topic stats of every route broker in
// parallel: offset tables overwrite per queue, put-TPS sums.
func (a *DefaultMQAdminExt) ExamineTopicStatsConcurrent(topic string) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		route, err := a.examineTopicRouteForFanout(topic)
		if err != nil {
			return nil, err
		}
		if len(route.BrokerDatas) == 0 {
			return remoting.NewTopicStatsTable(), nil
		}
		timeout := a.timeout(0)
		var mu sync.Mutex
		var parts []*remoting.TopicStatsTable
		var wg sync.WaitGroup
		for _, bd := range route.BrokerDatas {
			wg.Add(1)
			go func(bd *BrokerData) {
				defer wg.Done()
				addr, ok := bd.SelectBrokerAddr()
				if !ok {
					return
				}
				part, err := a.ExamineTopicStatsByBroker(addr, topic)
				if err != nil {
					common.LogWarnf("getTopicStatsInfo error. topic=%s: %v", topic, err)
					return
				}
				mu.Lock()
				parts = append(parts, part)
				mu.Unlock()
			}(bd)
		}
		awaitFanout(&wg, timeout)

		merged := remoting.NewTopicStatsTable()
		mu.Lock()
		for _, part := range parts {
			for q, off := range part.OffsetTable {
				merged.OffsetTable[q] = off
			}
			merged.TopicPutTps += part.TopicPutTps
		}
		mu.Unlock()
		return merged, nil
	})
}

// ---------------- examineConsumeStatsConcurrent ----------------

// ExamineConsumeStatsConcurrent merges the consume stats of every route
// broker, routing through the retry topic, the plain topic and the POP retry
// topic in turn. An empty offset table is turned into a consumer-not-online /
// broadcast verdict by probing the group's connections.
func (a *DefaultMQAdminExt) ExamineConsumeStatsConcurrent(consumerGroup, topic string) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		routeTopics := []string{common.GetRetryTopic(consumerGroup)}
		if topic != "" {
			routeTopics = append(routeTopics, topic, common.BuildPopRetryTopicV1(topic, consumerGroup))
		}
		var route *TopicRouteData
		for _, candidate := range routeTopics {
			r, err := a.examineTopicRouteForFanout(candidate)
			if err != nil {
				continue
			}
			route = r
			break
		}
		if route == nil || len(route.BrokerDatas) == 0 {
			return nil, &adminToolFailure{result: adminToolFailureResult(
				AdminToolTopicRouteInfoNotExist, "topic router info not found")}
		}
		timeout := a.timeout(0)
		var mu sync.Mutex
		var parts []*remoting.ConsumeStats
		var wg sync.WaitGroup
		for _, bd := range route.BrokerDatas {
			wg.Add(1)
			go func(bd *BrokerData) {
				defer wg.Done()
				addr, ok := bd.SelectBrokerAddr()
				if !ok {
					return
				}
				part, err := a.ExamineConsumeStats(addr, consumerGroup, topic, nil)
				if err != nil {
					common.LogWarnf("getConsumeStats error. topic=%s, consumerGroup=%s: %v", topic, consumerGroup, err)
					return
				}
				mu.Lock()
				parts = append(parts, part)
				mu.Unlock()
			}(bd)
		}
		awaitFanout(&wg, timeout)

		result := remoting.NewConsumeStats()
		mu.Lock()
		for _, part := range parts {
			for q, w := range part.OffsetTable {
				result.OffsetTable[q] = w
			}
			result.ConsumeTps += part.ConsumeTps
		}
		mu.Unlock()

		if len(result.OffsetTable) == 0 {
			connection, err := a.ExamineConsumerConnectionInfo(consumerGroup, "")
			if err != nil {
				return nil, &adminToolFailure{result: adminToolFailureResult(
					AdminToolConsumerNotOnline,
					"Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not consume any message")}
			}
			if connection.MessageModel == remoting.MessageModelBroadcasting {
				return nil, &adminToolFailure{result: adminToolFailureResult(
					AdminToolBroadcastConsumption,
					"Not found the consumer group consume stats, because return offset table is empty, the consumer is under the broadcast mode")}
			}
		}
		return result, nil
	})
}

// ---------------- deleteTopicInBrokerConcurrent ----------------

// DeleteTopicInBrokerConcurrent deletes the topic on every given broker in
// parallel and always reports which brokers accepted and which refused.
func (a *DefaultMQAdminExt) DeleteTopicInBrokerConcurrent(addrs []string, topic string) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		timeout := a.timeout(0)
		var mu sync.Mutex
		result := &BrokerOperatorResult{}
		var wg sync.WaitGroup
		for _, addr := range addrs {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				_, err := a.invokeBroker(addr, remoting.ReqDeleteTopicInBroker, adminExt("topic", topic), nil, timeout)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					common.LogWarnf("deleteTopicInBroker error. topic=%s, broker=%s: %v", topic, addr, err)
					result.FailureList = append(result.FailureList, addr)
					return
				}
				result.SuccessList = append(result.SuccessList, addr)
			}(addr)
		}
		awaitFanout(&wg, timeout)
		return result, nil
	})
}

// ---------------- resetOffsetNewConcurrent ----------------

// ResetOffsetNewConcurrent resets the offsets of every route broker in
// parallel. A broker without an online group falls back to the queue-by-queue
// rewrite, a never-online group counts as reset, and the envelope only
// reports success when EVERY route broker landed in the success list.
func (a *DefaultMQAdminExt) ResetOffsetNewConcurrent(group, topic string, timestamp int64) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		route, err := a.examineTopicRouteForFanout(topic)
		if err != nil {
			return nil, err
		}
		if len(route.BrokerDatas) == 0 {
			return nil, &adminToolFailure{result: adminToolFailureResult(
				AdminToolTopicRouteInfoNotExist, "topic router info not found")}
		}
		routeMap := map[string]*QueueData{}
		for _, qd := range route.QueueDatas {
			routeMap[qd.BrokerName] = qd
		}
		timeout := a.timeout(0)
		var mu sync.Mutex
		result := &BrokerOperatorResult{}
		var wg sync.WaitGroup
		for _, bd := range route.BrokerDatas {
			wg.Add(1)
			go func(bd *BrokerData) {
				defer wg.Done()
				addr, ok := bd.SelectBrokerAddr()
				if !ok {
					return
				}
				table, _, err := a.invokeBrokerResetOffsetDetailed(addr, topic, group, timestamp, true, false, nil, nil)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					if table != nil {
						result.SuccessList = append(result.SuccessList, addr)
					} else {
						result.FailureList = append(result.FailureList, addr)
					}
				case adminCodeIs(err, remoting.RespConsumerNotOnline):
					instance, cliErr := a.requireClient()
					if cliErr != nil {
						result.FailureList = append(result.FailureList, addr)
						return
					}
					if _, err2 := a.resetOffsetOldOneBroker(instance, addr, bd.BrokerName,
						routeMap[bd.BrokerName], group, topic, timestamp, true); err2 != nil {
						common.LogWarnf("resetOffsetByTimestampOld error. addr=%s, topic=%s, group=%s, timestamp=%d: %v",
							addr, topic, group, timestamp, err2)
						result.FailureList = append(result.FailureList, addr)
						return
					}
					result.SuccessList = append(result.SuccessList, addr)
				case adminCodeIs(err, remoting.RespSystemError):
					// CODE: 1  DESC: the consumer group ... not exist, never online —
					// nothing to reset, so the broker counts as done.
					result.SuccessList = append(result.SuccessList, addr)
				default:
					common.LogWarnf("resetOffsetNewConcurrent error. addr=%s, topic=%s, group=%s, timestamp=%d: %v",
						addr, topic, group, timestamp, err)
					result.FailureList = append(result.FailureList, addr)
				}
			}(bd)
		}
		awaitFanout(&wg, timeout)
		if len(result.SuccessList) == len(route.BrokerDatas) {
			return result, nil
		}
		return nil, &adminToolFailure{result: adminToolFailureResultWithData(
			AdminToolMQBrokerError, "operator failure", result)}
	})
}

// ---------------- queryTopicsByConsumerConcurrent ----------------

// QueryTopicsByConsumerConcurrent unions the topics every route broker still
// holds committed offsets for.
func (a *DefaultMQAdminExt) QueryTopicsByConsumerConcurrent(group string) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		route, err := a.examineTopicRouteForFanout(common.GetRetryTopic(group))
		if err != nil {
			return nil, err
		}
		if len(route.BrokerDatas) == 0 {
			return nil, &adminToolFailure{result: adminToolFailureResult(
				AdminToolTopicRouteInfoNotExist, "router info not found.")}
		}
		timeout := a.timeout(0)
		var mu sync.Mutex
		seen := map[string]bool{}
		var topics []string
		var wg sync.WaitGroup
		for _, bd := range route.BrokerDatas {
			wg.Add(1)
			go func(bd *BrokerData) {
				defer wg.Done()
				addr, ok := bd.SelectBrokerAddr()
				if !ok {
					return
				}
				part, err := a.QueryTopicsByConsumerToBroker(addr, group)
				if err != nil {
					common.LogWarnf("queryTopicsByConsumer error. group=%s: %v", group, err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, topic := range part.TopicList {
					if !seen[topic] {
						seen[topic] = true
						topics = append(topics, topic)
					}
				}
			}(bd)
		}
		awaitFanout(&wg, timeout)
		result := remoting.NewTopicList()
		result.TopicList = topics
		return result, nil
	})
}

// ---------------- queryConsumeTimeSpanConcurrent ----------------

// QueryConsumeTimeSpanConcurrent concatenates every route broker's consume
// window. Unlike the other fan-outs, a route without brokers is a SUCCESS
// with an empty set, not a missing route.
func (a *DefaultMQAdminExt) QueryConsumeTimeSpanConcurrent(topic, group string) *AdminToolResult {
	return adminToolExecute(func() (any, error) {
		route, err := a.examineTopicRouteForFanout(topic)
		if err != nil {
			return nil, err
		}
		if len(route.BrokerDatas) == 0 {
			return []*remoting.QueueTimeSpan{}, nil
		}
		timeout := a.timeout(0)
		var mu sync.Mutex
		var spans []*remoting.QueueTimeSpan
		var wg sync.WaitGroup
		for _, bd := range route.BrokerDatas {
			wg.Add(1)
			go func(bd *BrokerData) {
				defer wg.Done()
				addr, ok := bd.SelectBrokerAddr()
				if !ok {
					return
				}
				part, err := a.QueryConsumeTimeSpanFromBroker(addr, topic, group)
				if err != nil {
					common.LogWarnf("queryConsumeTimeSpan error. topic=%s, group=%s: %v", topic, group, err)
					return
				}
				mu.Lock()
				spans = append(spans, part...)
				mu.Unlock()
			}(bd)
		}
		awaitFanout(&wg, timeout)
		mu.Lock()
		defer mu.Unlock()
		return spans, nil
	})
}

// ---------------- messageTrackDetailConcurrent ----------------

// MessageTrackDetailConcurrent renders one track per consuming group in
// parallel; the per-group verdicts are the same as the sequential walk.
func (a *DefaultMQAdminExt) MessageTrackDetailConcurrent(msg *common.MessageExt) ([]*MessageTrack, error) {
	route, err := a.examineTopicRouteForFanout(msg.Topic)
	if err != nil {
		return nil, err
	}
	brokerAddr := ""
	for _, bd := range route.BrokerDatas {
		if addr, ok := bd.SelectBrokerAddr(); ok {
			brokerAddr = addr
			break
		}
	}
	if brokerAddr == "" {
		return []*MessageTrack{}, nil
	}
	groups, err := a.QueryTopicConsumeByWho(brokerAddr, msg.Topic)
	if err != nil {
		return nil, err
	}
	groupList := sortedStringSet(groups)
	timeout := a.timeout(0)
	var mu sync.Mutex
	tracks := make([]*MessageTrack, 0, len(groupList))
	var wg sync.WaitGroup
	for _, group := range groupList {
		wg.Add(1)
		go func(group string) {
			defer wg.Done()
			mt := a.trackOneGroup(msg, group)
			mu.Lock()
			tracks = append(tracks, mt)
			mu.Unlock()
		}(group)
	}
	awaitFanout(&wg, timeout)
	return tracks, nil
}
