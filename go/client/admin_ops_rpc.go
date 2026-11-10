// The #47 mqadmin ops RPC family: queryCorrectionOffset(308), the three
// unit-topic reads (311/312/313), getAllProducerInfo(328),
// getBrokerHAStatus(907), resetMasterFlushOffset(908), electMaster(1002),
// getBrokerEpochCache(1007) and switchTimerEngine(5001).
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - 308 and 328 are fast-channel RPCs: Java wraps them in
//     brokerVIPChannel(flag, addr), so the VIP switch moves their port.
//   - The three unit-topic reads go to a NAME SERVER with a null header (no
//     extFields at all); `containRetry` is never sent — when false the
//     %RETRY%-prefixed topics are stripped client-side after decoding.
//   - 908 / 1007 and both electMaster legs are pinned to a 3000ms budget;
//     the other calls take the caller's timeout.
//   - electMaster is two-phase: 1005 metadata read names the leader, then 1002
//     carries the admin-trigger header (designateElect=true, brokerId default
//     -1, invokeTime=now). The answer header rides the response extFields,
//     the member group is the body.
//   - switchTimerEngine has no header of its own; the engine type rides the
//     reserved `E_T` extField key.
package client

import (
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// QueryCorrectionOffset sends QUERY_CORRECTION_OFFSET(308) and returns the
// per-queue correction offsets of `group` against one broker. filterGroups is
// joined with commas; when empty the key is left off the wire entirely.
func (a *DefaultMQAdminExt) QueryCorrectionOffset(addr, topic, group string,
	filterGroups []string, timeoutMillis int64) (map[int32]int64, error) {

	header := &remoting.QueryCorrectionOffsetRequestHeader{
		Topic:        remoting.StrPtr(topic),
		CompareGroup: remoting.StrPtr(group),
	}
	if len(filterGroups) > 0 {
		header.FilterGroups = remoting.StrPtr(strings.Join(filterGroups, ","))
	}
	ext := common.NewStringMap()
	header.ToExtFields(ext)
	response, err := a.invokeBroker(addr, remoting.ReqQueryCorrectionOffset, ext, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientErrorCode(response.Code, response.Remark)
	}
	body, err := remoting.DecodeQueryCorrectionOffsetBody(response.Body)
	if err != nil {
		return nil, err
	}
	return body.CorrectionOffsets, nil
}

// getUnitTopicFamily sends one of the three unit-topic reads (311/312/313) to
// a name server and applies the local %RETRY% filter when containRetry is
// false. The header is null: no extFields ride the request.
func (a *DefaultMQAdminExt) getUnitTopicFamily(code int32, containRetry bool,
	timeoutMillis int64) (*remoting.TopicList, error) {

	response, err := a.invokeNameServerOne(code, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code != remoting.RespSuccess || len(response.Body) == 0 {
		return nil, common.ClientErrorCode(response.Code, response.Remark)
	}
	topics, err := remoting.DecodeTopicList(response.Body)
	if err != nil {
		return nil, err
	}
	if !containRetry {
		kept := topics.TopicList[:0]
		for _, topic := range topics.TopicList {
			if !strings.HasPrefix(topic, common.RetryGroupTopicPrefix) {
				kept = append(kept, topic)
			}
		}
		topics.TopicList = kept
	}
	return topics, nil
}

// GetUnitTopicList sends GET_UNIT_TOPIC_LIST(311).
func (a *DefaultMQAdminExt) GetUnitTopicList(containRetry bool, timeoutMillis int64) (*remoting.TopicList, error) {
	return a.getUnitTopicFamily(remoting.ReqGetUnitTopicList, containRetry, timeoutMillis)
}

// GetHasUnitSubTopicList sends GET_HAS_UNIT_SUB_TOPIC_LIST(312).
func (a *DefaultMQAdminExt) GetHasUnitSubTopicList(containRetry bool, timeoutMillis int64) (*remoting.TopicList, error) {
	return a.getUnitTopicFamily(remoting.ReqGetHasUnitSubTopicList, containRetry, timeoutMillis)
}

// GetHasUnitSubUnUnitTopicList sends GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST(313).
func (a *DefaultMQAdminExt) GetHasUnitSubUnUnitTopicList(containRetry bool, timeoutMillis int64) (*remoting.TopicList, error) {
	return a.getUnitTopicFamily(remoting.ReqGetHasUnitSubUnUnitTopicList, containRetry, timeoutMillis)
}

// GetAllProducerInfo sends GET_ALL_PRODUCER_INFO(328): the live producer
// connections of one broker, keyed by group. Fast-channel RPC.
func (a *DefaultMQAdminExt) GetAllProducerInfo(addr string, timeoutMillis int64) (*remoting.ProducerTableInfo, error) {
	ext := common.NewStringMap()
	(&remoting.GetAllProducerInfoRequestHeader{}).ToExtFields(ext)
	response, err := a.invokeBroker(addr, remoting.ReqGetAllProducerInfo, ext, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientErrorCode(response.Code, response.Remark)
	}
	return remoting.DecodeProducerTableInfo(response.Body)
}

// GetBrokerHAStatus sends GET_BROKER_HA_STATUS(907) to the listening port.
func (a *DefaultMQAdminExt) GetBrokerHAStatus(addr string, timeoutMillis int64) (*remoting.HARuntimeInfo, error) {
	response, err := a.invokeBrokerPlain(addr, remoting.ReqGetBrokerHAStatus, nil, nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientErrorCode(response.Code, response.Remark)
	}
	return remoting.DecodeHARuntimeInfo(response.Body)
}

// ResetMasterFlushOffset sends RESET_MASTER_FLUSH_OFFSET(908) on the fixed
// controller budget.
func (a *DefaultMQAdminExt) ResetMasterFlushOffset(addr string, masterFlushOffset int64) error {
	ext := common.NewStringMap()
	(&remoting.ResetMasterFlushOffsetRequestHeader{
		MasterFlushOffset: remoting.I64Ptr(masterFlushOffset),
	}).ToExtFields(ext)
	_, err := a.invokeBrokerPlain(addr, remoting.ReqResetMasterFlushOffset, ext, nil,
		controllerInvokeTimeoutMillis)
	return err
}

// ElectMaster runs the two-phase CONTROLLER_ELECT_MASTER(1002): the metadata
// read names the leader, then the admin-trigger request (designateElect=true,
// brokerId -1 unless designated, invokeTime=now) goes to the leader. Returns
// the response header (from the extFields) and the member group (the body),
// mirroring Java's Pair.
func (a *DefaultMQAdminExt) ElectMaster(controllerAddr, clusterName, brokerName string,
	brokerID int64) (*remoting.ElectMasterResponseHeader, *remoting.BrokerMemberGroup, error) {

	leader, err := a.controllerLeader(controllerAddr)
	if err != nil {
		return nil, nil, err
	}
	instance, err := a.requireClient()
	if err != nil {
		return nil, nil, err
	}
	ext := common.NewStringMap()
	(&remoting.ElectMasterRequestHeader{
		ClusterName:    remoting.StrPtr(clusterName),
		BrokerName:     remoting.StrPtr(brokerName),
		BrokerID:       remoting.I64Ptr(brokerID),
		DesignateElect: remoting.BoolPtr(true),
		InvokeTime:     remoting.I64Ptr(common.CurrentTimeMillis()),
	}).ToExtFields(ext)
	response, err := instance.invokeSync(leader,
		adminRequest(remoting.ReqControllerElectMaster, ext), controllerInvokeTimeoutMillis)
	if err != nil {
		return nil, nil, err
	}
	if err := instance.checkResponse(response); err != nil {
		return nil, nil, err
	}
	header := &remoting.ElectMasterResponseHeader{}
	header.FromExtFields(response.ExtFields())
	if len(response.Body) == 0 {
		return header, nil, nil
	}
	group, err := remoting.DecodeBrokerMemberGroup(response.Body)
	if err != nil {
		return nil, nil, err
	}
	return header, group, nil
}

// GetBrokerEpochCache sends GET_BROKER_EPOCH_CACHE(1007) on the fixed
// controller budget.
func (a *DefaultMQAdminExt) GetBrokerEpochCache(addr string) (*remoting.EpochEntryCache, error) {
	response, err := a.invokeBrokerPlain(addr, remoting.ReqGetBrokerEpochCache, nil, nil,
		controllerInvokeTimeoutMillis)
	if err != nil {
		return nil, err
	}
	if len(response.Body) == 0 {
		return nil, common.ClientErrorCode(response.Code, response.Remark)
	}
	return remoting.DecodeEpochEntryCache(response.Body)
}

// SwitchTimerEngine sends SWITCH_TIMER_ENGINE(5001); the engine type rides
// the reserved `E_T` extField key.
func (a *DefaultMQAdminExt) SwitchTimerEngine(addr, engineType string, timeoutMillis int64) error {
	_, err := a.invokeBrokerPlain(addr, remoting.ReqSwitchTimerEngine,
		adminExt("E_T", engineType), nil, timeoutMillis)
	return err
}

// controllerLeader resolves the 1005 leader address on the fixed controller
// budget (shared by the two-phase electMaster entry).
func (a *DefaultMQAdminExt) controllerLeader(controllerAddr string) (string, error) {
	meta, err := a.GetControllerMetaData(controllerAddr)
	if err != nil {
		return "", err
	}
	if meta.ControllerLeaderAddr == "" {
		return "", common.ClientError("controller leader address is empty")
	}
	return meta.ControllerLeaderAddr, nil
}
