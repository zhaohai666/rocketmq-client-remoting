package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// defaultViewMessageTimeoutMillis is Java MQAdminImpl's own RPC budget
// (MQAdminImpl.java:67, `timeoutMillis = 6000`): the producer / push / pull
// viewMessage facades run on it, not on the send timeout or the admin budget.
const defaultViewMessageTimeoutMillis = int64(6000)

// ViewMessage is the Java MQAdminImpl#viewMessage(topic, msgId) core
// (MQAdminImpl.java:267): a single leg — decode the offset msgId and ask the
// broker that stamped it VIEW_MESSAGE_BY_ID(33) straight away. Decode failure
// maps to the NO_MESSAGE client error Java raises there; every other leg
// failure surfaces as-is. The uniq-key fallback is NOT part of this layer —
// it belongs to the admin facade only (DefaultMQAdminExtImpl#viewMessage, see
// Admin.ViewMessage), while the producer / push / pull facades ride this
// single-leg path in Java.
func (i *Instance) ViewMessage(topic, msgID string, timeoutMillis int64) (*common.MessageExt, error) {
	ip, port, offset, err := common.DecodeMessageID(msgID)
	if err != nil {
		return nil, common.ClientErrorCode(remoting.RespNoMessage,
			"query message by id finished, but no message.")
	}
	if port == 0 || port > 65535 {
		return nil, common.ClientError(fmt.Sprintf("not a valid offset msgId: %s", msgID))
	}
	// The address is the store host:port the broker stamped into the offset
	// msgId — a broker address, so the VIP-channel rewrite applies to it like
	// to any other query request.
	addr := fmt.Sprintf("%s:%d", ip, port)
	response, err := i.invokeSync(i.brokerVIPAddr(addr), adminRequest(remoting.ReqViewMessageByID,
		adminExt("topic", topic, "offset", offset)), timeoutMillis)
	if err != nil {
		return nil, err
	}
	if err := i.checkResponse(response); err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.BrokerError(remoting.RespNoMessage,
			fmt.Sprintf("message not found: %s", msgID))
	}
	return common.DecodeMessage(response.Body)
}
