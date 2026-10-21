// Wire bodies and response headers of the controller / container / RocksDB
// admin family. They live next to the other protocol bodies so the client
// layer stays pure request choreography.
package remoting

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------- CONTROLLER_GET_METADATA_INFO (1005) response header ----------------

// ControllerMetaData is the GET_METADATA answer of a controller: who leads the
// raft group and where the peers live. The leader address is the routing key
// for the follow-up sync-state / clean-broker requests.
type ControllerMetaData struct {
	Group                string
	ControllerLeaderID   string
	ControllerLeaderAddr string
	IsLeader             bool
	Peers                string
}

func (m *ControllerMetaData) FromExtFields(ext *common.StringMap) {
	text := func(key string) string {
		if ext == nil {
			return ""
		}
		v, _ := ext.Get(key)
		return v
	}
	m.Group = text("group")
	m.ControllerLeaderID = text("controllerLeaderId")
	m.ControllerLeaderAddr = text("controllerLeaderAddress")
	m.IsLeader = text("isLeader") == "true"
	m.Peers = text("peers")
}

// ---------------- CONTROLLER_GET_SYNC_STATE_DATA (1006) body ----------------

// BrokerReplicasInfo is the per-broker replica state a controller reports.
type BrokerReplicasInfo struct {
	ReplicasInfoTable map[string]*ReplicasInfo
}

// ReplicasInfo is one broker's master and sync-state-set view.
type ReplicasInfo struct {
	MasterBrokerID    *int64
	MasterAddress     string
	MasterEpoch       *int32
	SyncStateSetEpoch *int32
	InSyncReplicas    []*ReplicaIdentity
	NotInSyncReplicas []*ReplicaIdentity
}

// ReplicaIdentity is one replica of a broker group.
type ReplicaIdentity struct {
	BrokerName    string
	BrokerID      *int64
	BrokerAddress string
	Alive         *bool
}

func (r *ReplicaIdentity) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ReplicaIdentity: expected object")
	}
	r.BrokerName = jsonStringOr(obj, "brokerName", "")
	r.BrokerAddress = jsonStringOr(obj, "brokerAddress", "")
	if raw, present := obj["brokerId"]; present && raw != nil {
		if id, ok := numberAsI64(raw); ok {
			id := id
			r.BrokerID = &id
		}
	}
	if raw, present := obj["alive"]; present && raw != nil {
		alive := jsonBool(obj, "alive", false)
		r.Alive = &alive
	}
	return nil
}

func (r *ReplicasInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ReplicasInfo: expected object")
	}
	if raw, present := obj["masterBrokerId"]; present && raw != nil {
		if id, ok := numberAsI64(raw); ok {
			id := id
			r.MasterBrokerID = &id
		}
	}
	r.MasterAddress = jsonStringOr(obj, "masterAddress", "")
	if raw, present := obj["masterEpoch"]; present && raw != nil {
		if n, ok := numberAsI64(raw); ok {
			epoch := int32(n)
			r.MasterEpoch = &epoch
		}
	}
	if raw, present := obj["syncStateSetEpoch"]; present && raw != nil {
		if n, ok := numberAsI64(raw); ok {
			epoch := int32(n)
			r.SyncStateSetEpoch = &epoch
		}
	}
	for _, item := range jsonArray(obj, "inSyncReplicas") {
		replica := &ReplicaIdentity{}
		if err := replica.FromJSONValue(item); err != nil {
			return err
		}
		r.InSyncReplicas = append(r.InSyncReplicas, replica)
	}
	for _, item := range jsonArray(obj, "notInSyncReplicas") {
		replica := &ReplicaIdentity{}
		if err := replica.FromJSONValue(item); err != nil {
			return err
		}
		r.NotInSyncReplicas = append(r.NotInSyncReplicas, replica)
	}
	return nil
}

func (b *BrokerReplicasInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("BrokerReplicasInfo: expected object")
	}
	b.ReplicasInfoTable = map[string]*ReplicasInfo{}
	table, _ := obj["replicasInfoTable"].(map[string]any)
	for brokerName, raw := range table {
		info := &ReplicasInfo{}
		if err := info.FromJSONValue(raw); err != nil {
			return err
		}
		b.ReplicasInfoTable[brokerName] = info
	}
	return nil
}

// DecodeBrokerReplicasInfo parses the 1006 body.
func DecodeBrokerReplicasInfo(data []byte) (*BrokerReplicasInfo, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	info := &BrokerReplicasInfo{}
	if err := info.FromJSONValue(value); err != nil {
		return nil, err
	}
	return info, nil
}

// ---------------- CHECK_ROCKSDB_CQ_WRITE_PROGRESS (354) body ----------------

// RocksdbCheckStatus values of CheckRocksdbCqWriteResult.checkStatus.
const (
	RocksdbCheckOK         = int32(0)
	RocksdbCheckNotOK      = int32(1)
	RocksdbCheckInProgress = int32(2)
	RocksdbCheckError      = int32(3)
)

// CheckRocksdbCqWriteResult is the 354 answer: whether the RocksDB consume
// queue had caught up with the commit log at checkStoreTime.
type CheckRocksdbCqWriteResult struct {
	CheckResult string
	CheckStatus int32
}

func (r *CheckRocksdbCqWriteResult) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("CheckRocksdbCqWriteResult: expected object")
	}
	r.CheckResult = jsonStringOr(obj, "checkResult", "")
	r.CheckStatus = jsonI32(obj, "checkStatus", 0)
	return nil
}

// DecodeCheckRocksdbCqWriteResult parses the 354 body.
func DecodeCheckRocksdbCqWriteResult(data []byte) (*CheckRocksdbCqWriteResult, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	result := &CheckRocksdbCqWriteResult{}
	if err := result.FromJSONValue(value); err != nil {
		return nil, err
	}
	return result, nil
}

// ReadAheadModeField is the extField key of SET_COMMITLOG_READ_MODE(2004).
const ReadAheadModeField = "READ_AHEAD_MODE"

// RocksDB config export type names of EXPORT_ROCKSDB_CONFIG_TO_JSON(355).
const (
	RocksDBConfigTopics            = "topics"
	RocksDBConfigSubscriptionGroup = "subscriptionGroups"
	RocksDBConfigConsumerOffsets   = "consumerOffsets"
)
