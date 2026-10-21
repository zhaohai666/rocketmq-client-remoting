// Lite-topic admin RPCs (200074-200079): the metadata queries the broker
// answers directly, plus the dispatch trigger. All of them go through ONE
// invoke helper so the VIP rewrite, timeout resolution and error surfacing
// stay identical across the family.
package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// liteDefaultMaxCount is the protocol default of GET_LITE_CLIENT_INFO's
// maxCount: a primitive field, so it is ALWAYS sent.
const liteDefaultMaxCount = 1000

// invokeBrokerMethod sends one broker admin request with the VIP rewrite and
// decodes the JSON object body when the broker sent one. A non-SUCCESS answer
// surfaces as a broker error carrying the requested address.
func (a *DefaultMQAdminExt) invokeBrokerMethod(addr string, code int32, ext *common.StringMap,
	timeoutMillis int64) (map[string]any, error) {

	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	response, err := instance.invokeSync(a.brokerVIPAddr(addr), adminRequest(code, ext), a.timeout(timeoutMillis))
	if err != nil {
		return nil, err
	}
	if response.Code != remoting.RespSuccess {
		errReply := common.BrokerError(response.Code, response.Remark)
		errReply.Addr = addr
		return nil, errReply
	}
	if len(response.Body) == 0 {
		return nil, nil
	}
	obj, ok := mustDecodeObject(response.Body)
	if !ok {
		return nil, common.ClientError(fmt.Sprintf("broker answer of code %d is not a JSON object", code))
	}
	return obj, nil
}

// GetBrokerLiteInfo sends GET_BROKER_LITE_INFO(200074).
func (a *DefaultMQAdminExt) GetBrokerLiteInfo(brokerAddr string, timeoutMillis int64) (map[string]any, error) {
	return a.invokeBrokerMethod(brokerAddr, remoting.ReqGetBrokerLiteInfo, nil, timeoutMillis)
}

// GetParentTopicInfo sends GET_PARENT_TOPIC_INFO(200075).
func (a *DefaultMQAdminExt) GetParentTopicInfo(brokerAddr, topic string, timeoutMillis int64) (map[string]any, error) {
	return a.invokeBrokerMethod(brokerAddr, remoting.ReqGetParentTopicInfo, adminExt("topic", topic), timeoutMillis)
}

// GetLiteTopicInfo sends GET_LITE_TOPIC_INFO(200076); an empty liteTopic is
// omitted (the header treats it as nullable).
func (a *DefaultMQAdminExt) GetLiteTopicInfo(brokerAddr, parentTopic, liteTopic string, timeoutMillis int64) (map[string]any, error) {
	ext := adminExt("parentTopic", parentTopic)
	if liteTopic != "" {
		ext.Put("liteTopic", liteTopic)
	}
	return a.invokeBrokerMethod(brokerAddr, remoting.ReqGetLiteTopicInfo, ext, timeoutMillis)
}

// GetLiteClientInfo sends GET_LITE_CLIENT_INFO(200077). maxCount is always
// sent with its protocol default.
func (a *DefaultMQAdminExt) GetLiteClientInfo(brokerAddr, parentTopic, group, clientID string, timeoutMillis int64) (map[string]any, error) {
	ext := adminExt("parentTopic", parentTopic, "group", group, "maxCount", liteDefaultMaxCount)
	if clientID != "" {
		ext.Put("clientId", clientID)
	}
	return a.invokeBrokerMethod(brokerAddr, remoting.ReqGetLiteClientInfo, ext, timeoutMillis)
}

// GetLiteGroupInfo sends GET_LITE_GROUP_INFO(200078). topK is a primitive
// field and always sent.
func (a *DefaultMQAdminExt) GetLiteGroupInfo(brokerAddr, group, liteTopic string, topK int32, timeoutMillis int64) (map[string]any, error) {
	ext := adminExt("group", group, "topK", topK)
	if liteTopic != "" {
		ext.Put("liteTopic", liteTopic)
	}
	return a.invokeBrokerMethod(brokerAddr, remoting.ReqGetLiteGroupInfo, ext, timeoutMillis)
}

// TriggerLiteDispatch sends TRIGGER_LITE_DISPATCH(200079); the broker answers
// without a body, so the result is only success/failure.
func (a *DefaultMQAdminExt) TriggerLiteDispatch(brokerAddr, group, clientID string, timeoutMillis int64) error {
	ext := adminExt("group", group)
	if clientID != "" {
		ext.Put("clientId", clientID)
	}
	_, err := a.invokeBrokerMethod(brokerAddr, remoting.ReqTriggerLiteDispatch, ext, timeoutMillis)
	return err
}
