// Cold-data flow control, commit-log read-ahead, POP record export and the
// RocksDB consume-queue admin RPCs.
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - UPDATE_COLD_DATA_FLOW_CTR_CONFIG(2001) / REMOVE_COLD_DATA_FLOW_CTR_CONFIG
//     (2002) ride the VIP-channel rewrite and carry properties TEXT / the raw
//     group string as body — and they are SKIPPED entirely when the payload is
//     empty (no request leaves the client).
//   - GET_COLD_DATA_FLOW_CTR_INFO(2003) never rewrites the port and returns the
//     raw body text; SET_COMMITLOG_READ_MODE(2004) carries the mode as a single
//     `READ_AHEAD_MODE` extField, also un-rewritten, and answers in the remark.
//   - POP_ROLLBACK(200056) forwards the CALLER's timeout verbatim — a zero does
//     not fall back to the admin default.
//   - CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354) always sends the primitive
//     `checkStoreTime`; EXPORT_ROCKSDB_CONFIG_TO_JSON(355) joins the type names
//     with ';' and keeps the TRAILING separator (`topics;subscriptionGroups;`).
package client

import (
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------- cold data flow control ----------------

// UpdateColdDataFlowCtrConfig sends UPDATE_COLD_DATA_FLOW_CTR_CONFIG(2001).
func (a *DefaultMQAdminExt) UpdateColdDataFlowCtrConfig(brokerAddr string, properties map[string]string, timeoutMillis int64) error {
	text := propertiesToString(properties)
	if text == "" {
		return nil
	}
	_, err := a.invokeBroker(brokerAddr, remoting.ReqUpdateColdDataFlowCtrConfig, nil, []byte(text), timeoutMillis)
	return err
}

// RemoveColdDataFlowCtrConfig sends REMOVE_COLD_DATA_FLOW_CTR_CONFIG(2002);
// the body is the group string itself, not a JSON wrapper.
func (a *DefaultMQAdminExt) RemoveColdDataFlowCtrConfig(brokerAddr, consumerGroup string, timeoutMillis int64) error {
	if consumerGroup == "" {
		return nil
	}
	_, err := a.invokeBroker(brokerAddr, remoting.ReqRemoveColdDataFlowCtrConfig, nil,
		[]byte(consumerGroup), timeoutMillis)
	return err
}

// GetColdDataFlowCtrInfo sends GET_COLD_DATA_FLOW_CTR_INFO(2003) and returns
// the raw body text ("" when the broker sent none).
func (a *DefaultMQAdminExt) GetColdDataFlowCtrInfo(brokerAddr string, timeoutMillis int64) (string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return "", err
	}
	response, err := instance.invokeSync(brokerAddr,
		adminRequest(remoting.ReqGetColdDataFlowCtrInfo, nil), a.timeout(timeoutMillis))
	if err != nil {
		return "", err
	}
	if err := instance.checkResponse(response); err != nil {
		return "", err
	}
	return string(response.Body), nil
}

// ---------------- commit-log read ahead ----------------

// SetCommitLogReadAheadMode sends SET_COMMITLOG_READ_MODE(2004) and returns
// the broker's remark ("" when it sent none).
func (a *DefaultMQAdminExt) SetCommitLogReadAheadMode(brokerAddr, mode string, timeoutMillis int64) (string, error) {
	instance, err := a.requireClient()
	if err != nil {
		return "", err
	}
	ext := common.NewStringMap()
	ext.Put(remoting.ReadAheadModeField, mode)
	response, err := instance.invokeSync(brokerAddr,
		adminRequest(remoting.ReqSetCommitLogReadMode, ext), a.timeout(timeoutMillis))
	if err != nil {
		return "", err
	}
	if err := instance.checkResponse(response); err != nil {
		return "", err
	}
	return response.Remark, nil
}

// ---------------- pop record export ----------------

// ExportPopRecords sends POP_ROLLBACK(200056): the broker dumps its POP
// in-flight records for backup. The caller's timeout is forwarded verbatim.
func (a *DefaultMQAdminExt) ExportPopRecords(brokerAddr string, timeoutMillis int64) error {
	instance, err := a.requireClient()
	if err != nil {
		return err
	}
	response, err := instance.invokeSync(brokerAddr,
		adminRequest(remoting.ReqPopRollback, nil), timeoutMillis)
	if err != nil {
		return err
	}
	return instance.checkResponse(response)
}

// ---------------- RocksDB consume queue ----------------

// CheckRocksdbCqWriteProgress sends CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354).
func (a *DefaultMQAdminExt) CheckRocksdbCqWriteProgress(brokerAddr, topic string, checkStoreTime, timeoutMillis int64) (*remoting.CheckRocksdbCqWriteResult, error) {
	response, err := a.invokeBrokerPlain(brokerAddr, remoting.ReqCheckRocksdbCqWriteProgress,
		adminExt("topic", topic, "checkStoreTime", checkStoreTime), nil, timeoutMillis)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeCheckRocksdbCqWriteResult(response.Body)
}

// ExportRocksDBConfigToJson sends EXPORT_ROCKSDB_CONFIG_TO_JSON(355) for the
// given config type names.
func (a *DefaultMQAdminExt) ExportRocksDBConfigToJson(brokerAddr string, configTypes []string, timeoutMillis int64) error {
	if len(configTypes) == 0 {
		return common.ClientError("exportRocksDBConfigToJson: configType must not be empty")
	}
	var sb strings.Builder
	for _, name := range configTypes {
		sb.WriteString(name)
		sb.WriteByte(';')
	}
	_, err := a.invokeBrokerPlain(brokerAddr, remoting.ReqExportRocksdbConfigToJSON,
		adminExt("configType", sb.String()), nil, timeoutMillis)
	return err
}
