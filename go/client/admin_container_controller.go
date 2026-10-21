// Broker-container and controller admin RPCs.
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - ADD_BROKER / REMOVE_BROKER (902/903) go to the CONTAINER address as
//     passed, never through the VIP-port rewrite, with a hardcoded 20000ms
//     timeout. 902's only extField is `configPath`; 903's are `brokerName`,
//     `brokerClusterName` and `brokerId`.
//   - The controller family is two-phase: CONTROLLER_GET_METADATA_INFO(1005)
//     against ANY controller member (3000ms hardcoded) returns the leader
//     address, and the follow-up requests go to the LEADER, again with 3000ms.
//   - CLEAN_BROKER_DATA(1011) always sends `isCleanLivingBroker` and
//     `invokeTime`; `clusterName` / `brokerControllerIdsToClean` are dropped
//     when empty (the header treats them as nullable strings).
//   - GET_CONTROLLER_CONFIG(1010) / UPDATE_CONTROLLER_CONFIG(1009) exchange
//     properties TEXT (`k=v\n`), not JSON. 1010 fans over every controller and
//     keeps the first failure as a thrown error; 1009 keeps the LAST failure.
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// controllerInvokeTimeoutMillis is the fixed controller timeout: a controller
// call neither inherits the admin timeout nor the caller's, so a slow admin
// setting cannot turn into a wedged controller round trip.
const controllerInvokeTimeoutMillis = int64(3000)

// containerInvokeTimeoutMillis is the fixed broker-container timeout.
const containerInvokeTimeoutMillis = int64(20000)

// ---------------- broker container ----------------

// AddBrokerToContainer sends ADD_BROKER(902): the container adopts the broker
// whose configuration lives at `brokerConfigPath`.
func (a *DefaultMQAdminExt) AddBrokerToContainer(brokerContainerAddr, brokerConfigPath string) error {
	_, err := a.invokeBrokerOn(brokerContainerAddr, remoting.ReqAddBroker,
		adminExt("configPath", brokerConfigPath), nil, containerInvokeTimeoutMillis)
	return err
}

// RemoveBrokerFromContainer sends REMOVE_BROKER(903).
func (a *DefaultMQAdminExt) RemoveBrokerFromContainer(brokerContainerAddr, clusterName, brokerName string, brokerID int64) error {
	_, err := a.invokeBrokerOn(brokerContainerAddr, remoting.ReqRemoveBroker,
		adminExt("brokerName", brokerName, "brokerClusterName", clusterName, "brokerId", brokerID),
		nil, containerInvokeTimeoutMillis)
	return err
}

// ---------------- controller metadata / sync state ----------------

// GetControllerMetaData sends CONTROLLER_GET_METADATA_INFO(1005) to one
// controller member and decodes the leader view from the response header.
func (a *DefaultMQAdminExt) GetControllerMetaData(controllerAddr string) (*remoting.ControllerMetaData, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	response, err := instance.invokeSync(controllerAddr,
		adminRequest(remoting.ReqControllerGetMetadataInfo, nil), controllerInvokeTimeoutMillis)
	if err != nil {
		return nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	meta := &remoting.ControllerMetaData{}
	meta.FromExtFields(response.ExtFields())
	return meta, nil
}

// GetInSyncStateData sends CONTROLLER_GET_SYNC_STATE_DATA(1006): the two-phase
// shape — metadata first, then the leader answers with the replica state of
// the requested brokers. The body is a raw JSON array of broker names.
func (a *DefaultMQAdminExt) GetInSyncStateData(controllerAddress string, brokers []string) (*remoting.BrokerReplicasInfo, error) {
	meta, err := a.GetControllerMetaData(controllerAddress)
	if err != nil {
		return nil, err
	}
	leader := meta.ControllerLeaderAddr
	if leader == "" {
		return nil, common.ClientError("controller leader address is empty")
	}
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	names := make([]any, 0, len(brokers))
	for _, b := range brokers {
		names = append(names, b)
	}
	request := adminRequest(remoting.ReqControllerGetSyncStateData, nil)
	request.SetBody(remoting.EncodeJSON(names))
	response, err := instance.invokeSync(leader, request, controllerInvokeTimeoutMillis)
	if err != nil {
		return nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, err
	}
	return remoting.DecodeBrokerReplicasInfo(response.Body)
}

// CleanControllerBrokerData sends CLEAN_BROKER_DATA(1011) to the controller
// leader: forget one broker's registration (optionally even while it lives).
func (a *DefaultMQAdminExt) CleanControllerBrokerData(controllerAddr, clusterName, brokerName,
	brokerControllerIdsToClean string, isCleanLivingBroker bool) error {

	meta, err := a.GetControllerMetaData(controllerAddr)
	if err != nil {
		return err
	}
	leader := meta.ControllerLeaderAddr
	if leader == "" {
		return common.ClientError("controller leader address is empty")
	}
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	ext := adminExt(
		"brokerName", brokerName,
		"isCleanLivingBroker", isCleanLivingBroker,
		// The timestamp makes every request unique, so a controller cannot
		// treat a retry as a duplicate of the earlier clean.
		"invokeTime", common.CurrentTimeMillis(),
	)
	if clusterName != "" {
		ext.Put("clusterName", clusterName)
	}
	if brokerControllerIdsToClean != "" {
		ext.Put("brokerControllerIdsToClean", brokerControllerIdsToClean)
	}
	response, err := instance.invokeSync(leader,
		adminRequest(remoting.ReqCleanBrokerData, ext), controllerInvokeTimeoutMillis)
	if err != nil {
		return err
	}
	return instance.checkResponse(response)
}

// ---------------- controller configuration ----------------

// GetControllerConfig sends GET_CONTROLLER_CONFIG(1010) to every controller
// (the nameserver list is the fallback target set) and parses each properties
// TEXT answer. The result is keyed by controller address.
func (a *DefaultMQAdminExt) GetControllerConfig(controllerServers []string, timeoutMillis int64) (map[string]map[string]string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return nil, err
	}
	targets := controllerServers
	if len(targets) == 0 {
		targets = instance.NameServerAddrs()
	}
	if len(targets) == 0 {
		return nil, nil
	}
	request := adminRequest(remoting.ReqGetControllerConfig, nil)
	out := make(map[string]map[string]string, len(targets))
	for _, controller := range targets {
		response, err := instance.invokeSync(controller, request, a.timeout(timeoutMillis))
		if err != nil {
			return nil, err
		}
		if response.Code != remoting.RespSuccess {
			return nil, common.ClientErrorCode(response.Code, response.Remark)
		}
		out[controller] = String2Properties(string(response.Body))
	}
	return out, nil
}

// UpdateControllerConfig sends UPDATE_CONTROLLER_CONFIG(1009) to every
// controller. An empty property set or an empty controller list is a silent
// no-op; every controller is still visited when one rejects, and the LAST
// rejection wins.
func (a *DefaultMQAdminExt) UpdateControllerConfig(properties map[string]string, controllers []string, timeoutMillis int64) error {
	text := propertiesToString(properties)
	if text == "" || len(controllers) == 0 {
		return nil
	}
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	request := adminRequest(remoting.ReqUpdateControllerConfig, nil)
	request.SetBody([]byte(text))
	var errResponse *remoting.RemotingCommand
	for _, controller := range controllers {
		response, err := instance.invokeSync(controller, request, a.timeout(timeoutMillis))
		if err != nil {
			return err
		}
		if response.Code != remoting.RespSuccess {
			errResponse = response
		}
	}
	if errResponse != nil {
		return common.ClientErrorCode(errResponse.Code, errResponse.Remark)
	}
	return nil
}
