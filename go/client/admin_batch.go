// The admin methods that were still missing versus
// org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl, ported together with
// the wire behaviour of org.apache.rocketmq.client.impl.MQClientAPIImpl.
//
// Wire facts established from the Java sources (do NOT "simplify" them):
//
//   - UPDATE_AND_CREATE_TOPIC_LIST(18) sends an EMPTY custom header; the whole
//     payload is the JSON body {"topicConfigList":[...]}. The Java header
//     class deliberately declares no fields (topic names are a body-level
//     resource for the authorization builder).
//   - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225) likewise sends an empty
//     header plus {"groupConfigList":[...]}. Note the field name differs from
//     the single-group variant, which is a bare SubscriptionGroupConfig object.
//   - UPDATE_AND_CREATE_STATIC_TOPIC(513) reuses CreateTopicRequestHeader
//     (topic/defaultTopic/readQueueNums/writeQueueNums/perm/topicFilterType/
//     topicSysFlag/order/force) and puts an encoded TopicQueueMappingDetail in
//     the body. It is a broker-local operation for unitized deployments.
//   - UPDATE_AND_GET_GROUP_FORBIDDEN(353) sends UpdateGroupForbiddenRequestHeader
//     (group/topic/readable) and decodes a GroupForbidden JSON body.
//   - RESUME_CHECK_HALF_MESSAGE(323) sends topic/msgId and maps any non-SUCCESS
//     reply to `false` rather than an error (Java MQClientAPIImpl:3279).
//   - createOrUpdateOrderConf is NOT a request of its own: it is a
//     read-modify-write over the nameserver KV namespace ORDER_TOPIC_CONFIG,
//     where the stored value is ";"-joined "key:value" entries.
package client

import (
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// NamespaceOrderTopicConfig is
// org.apache.rocketmq.common.namesrv.NamesrvUtil#NAMESPACE_ORDER_TOPIC_CONFIG.
const NamespaceOrderTopicConfig = "ORDER_TOPIC_CONFIG"

// TopicQueueMappingDetail mirrors
// org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail —
// the unitized-topic mapping document carried in a static-topic create.
type TopicQueueMappingDetail struct {
	Topic        string
	Scope        string
	TotalQueues  int32
	Bname        string
	Epoch        int64
	Dirty        bool
	CurrIDMap    map[int]int
	HostedQueues map[int][]LogicQueueMappingItem
}

// LogicQueueMappingItem mirrors
// org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem.
type LogicQueueMappingItem struct {
	Gen         int32
	QueueID     int32
	Bname       string
	LogicOffset int64
	StartOffset int64
	EndOffset   int64
	TimeOfStart int64
	TimeOfEnd   int64
}

func (d *TopicQueueMappingDetail) toJSONValue() map[string]any {
	curr := make(map[string]any, len(d.CurrIDMap))
	for k, v := range d.CurrIDMap {
		curr[itoa(k)] = v
	}
	hosted := make(map[string]any, len(d.HostedQueues))
	for globalID, items := range d.HostedQueues {
		rows := make([]any, 0, len(items))
		for _, item := range items {
			rows = append(rows, map[string]any{
				"gen":         item.Gen,
				"queueId":     item.QueueID,
				"bname":       item.Bname,
				"logicOffset": item.LogicOffset,
				"startOffset": item.StartOffset,
				"endOffset":   item.EndOffset,
				"timeOfStart": item.TimeOfStart,
				"timeOfEnd":   item.TimeOfEnd,
			})
		}
		hosted[itoa(globalID)] = rows
	}
	scope := d.Scope
	if scope == "" {
		// MixAll.METADATA_SCOPE_GLOBAL
		scope = "GLOBAL"
	}
	return map[string]any{
		"topic":        d.Topic,
		"scope":        scope,
		"totalQueues":  d.TotalQueues,
		"bname":        d.Bname,
		"epoch":        d.Epoch,
		"dirty":        d.Dirty,
		"currIdMap":    curr,
		"hostedQueues": hosted,
	}
}

// Encode renders the body the broker decodes as a TopicQueueMappingDetail.
func (d *TopicQueueMappingDetail) Encode() []byte {
	return remoting.EncodeJSON(d.toJSONValue())
}

// GroupForbidden mirrors
// org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden.
type GroupForbidden struct {
	Topic    string
	Group    string
	Readable bool
}

func (g *GroupForbidden) toJSONValue() map[string]any {
	return map[string]any{
		"topic":    g.Topic,
		"group":    g.Group,
		"readable": g.Readable,
	}
}

// DecodeGroupForbidden parses a GroupForbidden body.
func DecodeGroupForbidden(data []byte) (*GroupForbidden, error) {
	value, err := remoting.DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, common.ClientError("GroupForbidden: expected a JSON object")
	}
	return &GroupForbidden{
		Topic:    jsonString(obj, "topic", ""),
		Group:    jsonString(obj, "group", ""),
		Readable: jsonBoolField(obj, "readable", false),
	}, nil
}

// ---------------- Batch configuration ----------------

// CreateAndUpdateTopicConfigList sends UPDATE_AND_CREATE_TOPIC_LIST(18).
// The request carries an empty custom header; the body is
// CreateTopicListRequestBody JSON, i.e. {"topicConfigList":[...]}.
func (a *DefaultMQAdminExt) CreateAndUpdateTopicConfigList(brokerAddr string,
	configs []*remoting.TopicConfig) error {
	if len(configs) == 0 {
		return common.ClientError("createAndUpdateTopicConfigList: empty topicConfigList")
	}
	rows := make([]any, 0, len(configs))
	for _, config := range configs {
		if config == nil {
			return common.ClientError("createAndUpdateTopicConfigList: nil TopicConfig in list")
		}
		rows = append(rows, config.ToJSONValue())
	}
	body := remoting.EncodeJSON(map[string]any{"topicConfigList": rows})
	_, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateAndCreateTopicList, nil, body, 0)
	return err
}

// CreateAndUpdateSubscriptionGroupConfigList sends
// UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225). Same shape as the topic-list
// variant, but the body key is `groupConfigList`.
func (a *DefaultMQAdminExt) CreateAndUpdateSubscriptionGroupConfigList(brokerAddr string,
	configs []*remoting.SubscriptionGroupConfig) error {
	if len(configs) == 0 {
		return common.ClientError("createAndUpdateSubscriptionGroupConfigList: empty groupConfigList")
	}
	rows := make([]any, 0, len(configs))
	for _, config := range configs {
		if config == nil {
			return common.ClientError("createAndUpdateSubscriptionGroupConfigList: nil SubscriptionGroupConfig in list")
		}
		rows = append(rows, config.ToJSONValue())
	}
	body := remoting.EncodeJSON(map[string]any{"groupConfigList": rows})
	_, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateAndCreateSubscriptionGrpLst, nil, body, 0)
	return err
}

// CreateStaticTopic sends UPDATE_AND_CREATE_STATIC_TOPIC(513): a
// CreateTopicRequestHeader in the custom header plus an encoded
// TopicQueueMappingDetail in the body.
func (a *DefaultMQAdminExt) CreateStaticTopic(brokerAddr, defaultTopic string,
	config *remoting.TopicConfig, mapping *TopicQueueMappingDetail, force bool) error {
	if config == nil {
		return common.ClientError("createStaticTopic: nil TopicConfig")
	}
	if mapping == nil {
		return common.ClientError("createStaticTopic: nil TopicQueueMappingDetail")
	}
	filterType := config.TopicFilterType
	if filterType == "" {
		filterType = remoting.TopicFilterTypeSingleTag
	}
	header := adminExt(
		"topic", config.TopicName,
		"defaultTopic", defaultTopic,
		"readQueueNums", config.ReadQueueNums,
		"writeQueueNums", config.WriteQueueNums,
		"perm", config.Perm,
		"topicFilterType", filterType,
		"topicSysFlag", config.TopicSysFlag,
		"order", config.Order,
		"force", force,
	)
	_, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateAndCreateStaticTopic,
		header, mapping.Encode(), 0)
	return err
}

// UpdateAndGetGroupReadForbidden sends UPDATE_AND_GET_GROUP_FORBIDDEN(353) and
// returns the decoded GroupForbidden. A nil `readable` means "query only",
// which is what Java sends when the caller does not want to change the flag.
func (a *DefaultMQAdminExt) UpdateAndGetGroupReadForbidden(brokerAddr, group, topic string,
	readable *bool) (*GroupForbidden, error) {
	if group == "" {
		return nil, common.ClientError("updateAndGetGroupReadForbidden: group must not be empty")
	}
	if topic == "" {
		return nil, common.ClientError("updateAndGetGroupReadForbidden: topic must not be empty")
	}
	header := adminExt("group", group, "topic", topic)
	if readable != nil {
		header.Put("readable", strconv.FormatBool(*readable))
	}
	response, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateAndGetGroupForbidden,
		header, nil, 0)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientError("updateAndGetGroupReadForbidden: empty response body")
	}
	return DecodeGroupForbidden(response.Body)
}

// ResumeCheckHalfMessage sends RESUME_CHECK_HALF_MESSAGE(323) so the broker
// re-runs the half-message check for one message. Java maps every non-SUCCESS
// reply to `false` instead of raising (MQClientAPIImpl:3279) — network errors
// still raise, but a broker-level rejection (e.g. the msgId is not a half
// message, which surfaces as SYSTEM_ERROR) is reported through the boolean.
// invokeBroker's checkResponse would turn that rejection into an error, so
// this inspects the raw reply.
func (a *DefaultMQAdminExt) ResumeCheckHalfMessage(brokerAddr, topic, msgID string) (bool, error) {
	if topic == "" {
		return false, common.ClientError("resumeCheckHalfMessage: topic must not be empty")
	}
	header := adminExt("topic", topic)
	if msgID != "" {
		header.Put("msgId", msgID)
	}
	instance, err := a.requireClient()
	if err != nil {
		return false, err
	}
	target := common.BrokerVIPChannel(a.vipChannel(), brokerAddr)
	request := adminRequest(remoting.ReqResumeCheckHalfMessage, header)
	response, err := instance.invokeSync(target, request, a.timeout(0))
	if err != nil {
		return false, err
	}
	return response.Code == remoting.RespSuccess, nil
}

// ---------------- Order topic configuration ----------------

// CreateOrUpdateOrderConf mirrors
// DefaultMQAdminExtImpl.createOrUpdateOrderConf: a read-modify-write over the
// nameserver KV namespace ORDER_TOPIC_CONFIG.
//
// With isCluster the value is stored verbatim. Otherwise the stored value is
// treated as a ";"-joined list of "key:value" entries, the entry whose key
// matches `value`'s key is replaced, and the whole list is written back — so
// one call touches a single topic instead of the whole cluster.
func (a *DefaultMQAdminExt) CreateOrUpdateOrderConf(key, value string, isCluster bool) error {
	if key == "" {
		return common.ClientError("createOrUpdateOrderConf: key must not be empty")
	}
	if value == "" {
		return common.ClientError("createOrUpdateOrderConf: value must not be empty")
	}
	if isCluster {
		return a.PutKVConfig(NamespaceOrderTopicConfig, key, value)
	}

	oldValue, _, err := a.GetKVConfig(NamespaceOrderTopicConfig, key)
	if err != nil {
		// Java prints and continues with an empty map: a missing key is the
		// normal first-write case, not a failure.
		oldValue = ""
	}

	// entry key -> full "key:value" text, preserving Java's HashMap semantics
	// (last write for a duplicated key wins).
	entries := map[string]string{}
	order := []string{}
	if strings.TrimSpace(oldValue) != "" {
		for _, entry := range strings.Split(oldValue, ";") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			entryKey := entry
			if idx := strings.Index(entry, ":"); idx >= 0 {
				entryKey = entry[:idx]
			}
			if _, seen := entries[entryKey]; !seen {
				order = append(order, entryKey)
			}
			entries[entryKey] = entry
		}
	}

	newKey := value
	if idx := strings.Index(value, ":"); idx >= 0 {
		newKey = value[:idx]
	}
	if newKey == "" {
		return common.ClientError("createOrUpdateOrderConf: value must start with a key")
	}
	if _, seen := entries[newKey]; !seen {
		order = append(order, newKey)
	}
	entries[newKey] = value

	merged := make([]string, 0, len(order))
	for _, entryKey := range order {
		merged = append(merged, entries[entryKey])
	}
	return a.PutKVConfig(NamespaceOrderTopicConfig, key, strings.Join(merged, ";"))
}

// ---------------- Housekeeping ----------------

// CleanExpiredConsumerQueue sends CLEAN_EXPIRED_CONSUMEQUEUE(306): the broker
// drops consume-queue entries older than `time` hours.
func (a *DefaultMQAdminExt) CleanExpiredConsumerQueue(brokerAddr string, time int32) error {
	_, err := a.invokeBroker(brokerAddr, remoting.ReqCleanExpiredConsumeQueue,
		adminExt("time", time), nil, 0)
	return err
}

// CleanExpiredConsumerQueueByAddr is the multi-broker form
// (DefaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr): every address is
// visited and the addresses that failed are returned together, so one dead
// broker does not hide the others' results.
func (a *DefaultMQAdminExt) CleanExpiredConsumerQueueByAddr(addrs []string, time int32) []string {
	var failed []string
	for _, addr := range addrs {
		if err := a.CleanExpiredConsumerQueue(addr, time); err != nil {
			failed = append(failed, addr)
		}
	}
	return failed
}

// DeleteExpiredCommitLog sends DELETE_EXPIRED_COMMITLOG(329): the broker
// deletes commit-log files older than `time` hours.
func (a *DefaultMQAdminExt) DeleteExpiredCommitLog(brokerAddr string, time int32) error {
	_, err := a.invokeBroker(brokerAddr, remoting.ReqDeleteExpiredCommitLog,
		adminExt("time", time), nil, 0)
	return err
}

// DeleteExpiredCommitLogByAddr is the multi-broker form.
func (a *DefaultMQAdminExt) DeleteExpiredCommitLogByAddr(addrs []string, time int32) []string {
	var failed []string
	for _, addr := range addrs {
		if err := a.DeleteExpiredCommitLog(addr, time); err != nil {
			failed = append(failed, addr)
		}
	}
	return failed
}

// CleanUnusedTopicByAddr mirrors
// DefaultMQAdminExtImpl.cleanUnusedTopicByAddr → MQClientAPIImpl:2696: ONE
// CLEAN_UNUSED_TOPIC(316) request that tells the broker to drop its own unused
// topics. The client does NOT walk the topic table and delete one by one —
// that would race the broker's own bookkeeping and trip over the broker-created
// topics (BenchmarkTest, retry/DLQ topics) the broker refuses to delete
// (SYSTEM_ERROR "conflict with system topic"). Java raises on non-SUCCESS here,
// which invokeBroker already does.
func (a *DefaultMQAdminExt) CleanUnusedTopicByAddr(brokerAddr string) error {
	_, err := a.invokeBroker(brokerAddr, remoting.ReqCleanUnusedTopic, nil, nil, 0)
	return err
}

// SetMessageRequestMode sends SET_MESSAGE_REQUEST_MODE(401), switching a
// POP consumer group between POP and pull mode (unitized deployments).
//
// Java `MQClientAPIImpl:3304-3311` builds the request with a **null header** and
// puts all four fields in the body: the broker's `AdminBrokerProcessor` decodes a
// `SetMessageRequestModeRequestBody` and never reads an ext field, so a header-only
// request makes it NPE on a null body. Same wire shape as the consumer-side
// `Instance.SetMessageRequestMode`.
func (a *DefaultMQAdminExt) SetMessageRequestMode(brokerAddr, topic, consumerGroup string,
	mode string, popShareQueueNum int32) error {
	body := (&remoting.SetMessageRequestModeRequestBody{
		Topic:            topic,
		ConsumerGroup:    consumerGroup,
		Mode:             mode,
		PopShareQueueNum: popShareQueueNum,
	}).Encode()
	_, err := a.invokeBroker(brokerAddr, remoting.ReqSetMessageRequestMode, nil, body, 0)
	return err
}

// GetTopicClusterList already lives in admin.go (it delegates to
// GetClusterList, the client-side derivation Java performs with
// EXAMINE_BROKER_CLUSTER_INFO(25) + EXAMINE_TOPIC_ROUTE(105) — no request of
// its own). Nothing to add here.
