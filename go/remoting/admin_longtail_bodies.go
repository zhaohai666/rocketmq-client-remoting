// Wire bodies and response headers of the controller / container / RocksDB
// admin family. They live next to the other protocol bodies so the client
// layer stays pure request choreography.
package remoting

import (
	"math"
	"strconv"

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

// ---------------- mqadmin ops family (#47) response bodies ----------------

// QueryCorrectionOffsetBody is the 308 answer: the per-queue correction
// offsets of `compareGroup`. The wire is a JSON object whose keys are the
// queue ids in string form, so it decodes through the string-keyed map.
type QueryCorrectionOffsetBody struct {
	CorrectionOffsets map[int32]int64
}

func (b *QueryCorrectionOffsetBody) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("QueryCorrectionOffsetBody: expected object")
	}
	b.CorrectionOffsets = map[int32]int64{}
	table, _ := obj["correctionOffsets"].(map[string]any)
	for key, raw := range table {
		queueID, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		if offset, ok := numberAsI64(raw); ok {
			b.CorrectionOffsets[int32(queueID)] = offset
		}
	}
	return nil
}

// DecodeQueryCorrectionOffsetBody parses the 308 body.
func DecodeQueryCorrectionOffsetBody(data []byte) (*QueryCorrectionOffsetBody, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	body := &QueryCorrectionOffsetBody{}
	if err := body.FromJSONValue(value); err != nil {
		return nil, err
	}
	return body, nil
}

// ProducerInfo is one live producer connection inside the 328 table.
type ProducerInfo struct {
	ClientID            string
	RemoteIP            string
	Language            string
	Version             int32
	LastUpdateTimestamp int64
}

func (p *ProducerInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ProducerInfo: expected object")
	}
	p.ClientID = jsonStringOr(obj, "clientId", "")
	p.RemoteIP = jsonStringOr(obj, "remoteIP", "")
	p.Language = jsonStringOr(obj, "language", "")
	p.Version = jsonI32(obj, "version", 0)
	p.LastUpdateTimestamp = jsonI64(obj, "lastUpdateTimestamp", 0)
	return nil
}

// ProducerTableInfo is the 328 answer: producer connections keyed by group.
type ProducerTableInfo struct {
	Data map[string][]*ProducerInfo
}

func (t *ProducerTableInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ProducerTableInfo: expected object")
	}
	t.Data = map[string][]*ProducerInfo{}
	table, _ := obj["data"].(map[string]any)
	for group, raw := range table {
		items, ok := raw.([]any)
		if !ok {
			continue
		}
		list := make([]*ProducerInfo, 0, len(items))
		for _, item := range items {
			info := &ProducerInfo{}
			if err := info.FromJSONValue(item); err != nil {
				return err
			}
			list = append(list, info)
		}
		t.Data[group] = list
	}
	return nil
}

// DecodeProducerTableInfo parses the 328 body.
func DecodeProducerTableInfo(data []byte) (*ProducerTableInfo, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	table := &ProducerTableInfo{}
	if err := table.FromJSONValue(value); err != nil {
		return nil, err
	}
	return table, nil
}

// HAConnectionRuntimeInfo is one broker-side HA connection of the 907 answer.
type HAConnectionRuntimeInfo struct {
	Addr                    string
	SlaveAckOffset          int64
	Diff                    int64
	InSync                  bool
	TransferredByteInSecond int64
	TransferFromWhere       int64
}

func (c *HAConnectionRuntimeInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("HAConnectionRuntimeInfo: expected object")
	}
	c.Addr = jsonStringOr(obj, "addr", "")
	c.SlaveAckOffset = jsonI64(obj, "slaveAckOffset", 0)
	c.Diff = jsonI64(obj, "diff", 0)
	c.InSync = jsonBool(obj, "inSync", false)
	c.TransferredByteInSecond = jsonI64(obj, "transferredByteInSecond", 0)
	c.TransferFromWhere = jsonI64(obj, "transferFromWhere", 0)
	return nil
}

// HAClientRuntimeInfo is the HA client leg of the 907 answer.
type HAClientRuntimeInfo struct {
	MasterAddr              string
	TransferredByteInSecond int64
	MaxOffset               int64
	LastReadTimestamp       int64
	LastWriteTimestamp      int64
	MasterFlushOffset       int64
	Activated               bool
}

func (c *HAClientRuntimeInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("HAClientRuntimeInfo: expected object")
	}
	c.MasterAddr = jsonStringOr(obj, "masterAddr", "")
	c.TransferredByteInSecond = jsonI64(obj, "transferredByteInSecond", 0)
	c.MaxOffset = jsonI64(obj, "maxOffset", 0)
	c.LastReadTimestamp = jsonI64(obj, "lastReadTimestamp", 0)
	c.LastWriteTimestamp = jsonI64(obj, "lastWriteTimestamp", 0)
	c.MasterFlushOffset = jsonI64(obj, "masterFlushOffset", 0)
	// fastjson writes the Java isActivated property as "activated"; older
	// writers spell the key out as "isActivated", so both are accepted.
	c.Activated = jsonBool(obj, "activated", jsonBool(obj, "isActivated", false))
	return nil
}

// HARuntimeInfo is the 907 answer: the broker's HA replication view.
type HARuntimeInfo struct {
	Master                   bool
	MasterCommitLogMaxOffset int64
	InSyncSlaveNums          int32
	HAConnectionInfo         []*HAConnectionRuntimeInfo
	HAClientRuntimeInfo      *HAClientRuntimeInfo
}

func (h *HARuntimeInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("HARuntimeInfo: expected object")
	}
	h.Master = jsonBool(obj, "master", false)
	h.MasterCommitLogMaxOffset = jsonI64(obj, "masterCommitLogMaxOffset", 0)
	h.InSyncSlaveNums = jsonI32(obj, "inSyncSlaveNums", 0)
	for _, item := range jsonArray(obj, "haConnectionInfo") {
		conn := &HAConnectionRuntimeInfo{}
		if err := conn.FromJSONValue(item); err != nil {
			return err
		}
		h.HAConnectionInfo = append(h.HAConnectionInfo, conn)
	}
	if raw, present := obj["haClientRuntimeInfo"]; present && raw != nil {
		client := &HAClientRuntimeInfo{}
		if err := client.FromJSONValue(raw); err != nil {
			return err
		}
		h.HAClientRuntimeInfo = client
	}
	return nil
}

// DecodeHARuntimeInfo parses the 907 body.
func DecodeHARuntimeInfo(data []byte) (*HARuntimeInfo, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	info := &HARuntimeInfo{}
	if err := info.FromJSONValue(value); err != nil {
		return nil, err
	}
	return info, nil
}

// LastEpochEndOffset is the endOffset a live (still-open) epoch reports; the
// wire simply omits the key, so the decoder fills it in.
const LastEpochEndOffset = int64(math.MaxInt64)

// EpochEntry is one controller epoch of the 1007 answer.
type EpochEntry struct {
	Epoch       int32
	StartOffset int64
	EndOffset   int64
}

func (e *EpochEntry) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("EpochEntry: expected object")
	}
	e.Epoch = jsonI32(obj, "epoch", 0)
	e.StartOffset = jsonI64(obj, "startOffset", 0)
	e.EndOffset = jsonI64(obj, "endOffset", LastEpochEndOffset)
	return nil
}

// EpochEntryCache is the 1007 answer: the broker's epoch list.
type EpochEntryCache struct {
	ClusterName string
	BrokerName  string
	BrokerID    int64
	EpochList   []*EpochEntry
	MaxOffset   int64
}

func (c *EpochEntryCache) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("EpochEntryCache: expected object")
	}
	c.ClusterName = jsonStringOr(obj, "clusterName", "")
	c.BrokerName = jsonStringOr(obj, "brokerName", "")
	c.BrokerID = jsonI64(obj, "brokerId", 0)
	for _, item := range jsonArray(obj, "epochList") {
		entry := &EpochEntry{}
		if err := entry.FromJSONValue(item); err != nil {
			return err
		}
		c.EpochList = append(c.EpochList, entry)
	}
	c.MaxOffset = jsonI64(obj, "maxOffset", 0)
	return nil
}

// DecodeEpochEntryCache parses the 1007 body.
func DecodeEpochEntryCache(data []byte) (*EpochEntryCache, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	cache := &EpochEntryCache{}
	if err := cache.FromJSONValue(value); err != nil {
		return nil, err
	}
	return cache, nil
}

// BrokerMemberGroup is the 1002 answer body: where the members of one broker
// group live. The wire keys the address map by brokerId in string form.
type BrokerMemberGroup struct {
	Cluster     string
	BrokerName  string
	BrokerAddrs map[int64]string
}

func (g *BrokerMemberGroup) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("BrokerMemberGroup: expected object")
	}
	g.Cluster = jsonStringOr(obj, "cluster", "")
	g.BrokerName = jsonStringOr(obj, "brokerName", "")
	g.BrokerAddrs = map[int64]string{}
	table, _ := obj["brokerAddrs"].(map[string]any)
	for key, raw := range table {
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			continue
		}
		if addr, ok := raw.(string); ok {
			g.BrokerAddrs[id] = addr
		}
	}
	return nil
}

// DecodeBrokerMemberGroup parses the 1002 body.
func DecodeBrokerMemberGroup(data []byte) (*BrokerMemberGroup, error) {
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	group := &BrokerMemberGroup{}
	if err := group.FromJSONValue(value); err != nil {
		return nil, err
	}
	return group, nil
}
