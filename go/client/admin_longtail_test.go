// Wire-shape tests for the broker-container, controller, cold-data,
// read-ahead, POP-export and RocksDB admin RPCs.
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func TestAdminLongtailTimeoutConstants(t *testing.T) {
	if containerInvokeTimeoutMillis != 20000 {
		t.Errorf("container timeout = %d, want 20000", containerInvokeTimeoutMillis)
	}
	if controllerInvokeTimeoutMillis != 3000 {
		t.Errorf("controller timeout = %d, want 3000", controllerInvokeTimeoutMillis)
	}
}

// newRecordingServer starts a mock that records everything it is sent and
// answers through `answer`.
func newRecordingServer(t *testing.T, answer func(req *remoting.RemotingCommand) *remoting.RemotingCommand) (*mockServer, *reqLog) {
	t.Helper()
	log := &reqLog{}
	srv := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		log.add(req)
		return answer(req)
	})
	return srv, log
}

// ---------------- broker container ----------------

func TestAdminAddBrokerToContainerWireShape(t *testing.T) {
	c := newAdminCluster(t)
	containerSrv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})
	// The container address is used as passed: even with the VIP switch on the
	// request must land on the given port, never on port-2 of it.
	c.admin.SetVipChannelEnabled(true)
	requireNoError(t, "addBrokerToContainer",
		c.admin.AddBrokerToContainer(containerSrv.addr, "/home/admin/broker.conf"))

	req := log.first(remoting.ReqAddBroker)
	if req == nil {
		t.Fatal("no ADD_BROKER request reached the container")
	}
	if got := ext(t, req, "configPath"); got != "/home/admin/broker.conf" {
		t.Errorf("configPath = %q", got)
	}
	if req.ExtFields().Len() != 1 {
		t.Errorf("extFields = %v, want only configPath", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("ADD_BROKER must not carry a body, got %q", string(req.Body))
	}
	if got := log.count(remoting.ReqAddBroker); got != 1 {
		t.Fatalf("ADD_BROKER sent %d times, want 1", got)
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "container busy")
	})
	err := c.admin.AddBrokerToContainer(rejected.addr, "/home/admin/broker.conf")
	if err == nil {
		t.Fatal("a rejected ADD_BROKER must surface the container error")
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Fatalf("error must carry the container code, got %v", err)
	}
}

func TestAdminRemoveBrokerFromContainerWireShape(t *testing.T) {
	c := newAdminCluster(t)
	containerSrv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})
	c.admin.SetVipChannelEnabled(true)
	requireNoError(t, "removeBrokerFromContainer",
		c.admin.RemoveBrokerFromContainer(containerSrv.addr, "DefaultCluster", "b1", 1))

	req := log.first(remoting.ReqRemoveBroker)
	if req == nil {
		t.Fatal("no REMOVE_BROKER request reached the container")
	}
	for key, want := range map[string]string{
		"brokerName": "b1", "brokerClusterName": "DefaultCluster", "brokerId": "1",
	} {
		if got := ext(t, req, key); got != want {
			t.Errorf("REMOVE_BROKER %s = %q, want %q", key, got, want)
		}
	}
	if req.ExtFields().Len() != 3 {
		t.Errorf("extFields = %v, want exactly the three header fields", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("REMOVE_BROKER must not carry a body, got %q", string(req.Body))
	}
}

// ---------------- controller metadata / sync state ----------------

func newControllerMetaAnswer(leaderAddr string) func(*remoting.RemotingCommand) *remoting.RemotingCommand {
	return func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqControllerGetMetadataInfo {
			return okCode(remoting.RespRequestCodeNotSupported, "meta only")
		}
		resp := respOK(nil)
		resp.AddExtField("group", "raft-group")
		resp.AddExtField("controllerLeaderId", "n1")
		resp.AddExtField("controllerLeaderAddress", leaderAddr)
		resp.AddExtField("isLeader", "true")
		resp.AddExtField("peers", "n1:1;n2:2")
		return resp
	}
}

func TestAdminGetControllerMetaDataDecodesLeaderHeader(t *testing.T) {
	c := newAdminCluster(t)
	leaderSrv, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})
	metaSrv, metaLog := newRecordingServer(t, newControllerMetaAnswer(leaderSrv.addr))

	meta, err := c.admin.GetControllerMetaData(metaSrv.addr)
	requireNoError(t, "getControllerMetaData", err)
	if meta.Group != "raft-group" || meta.ControllerLeaderID != "n1" ||
		meta.ControllerLeaderAddr != leaderSrv.addr || !meta.IsLeader ||
		meta.Peers != "n1:1;n2:2" {
		t.Fatalf("meta = %+v", meta)
	}
	if metaLog.count(remoting.ReqControllerGetMetadataInfo) != 1 {
		t.Fatalf("metadata requests = %d, want 1", metaLog.count(remoting.ReqControllerGetMetadataInfo))
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "no quorum")
	})
	if _, err := c.admin.GetControllerMetaData(rejected.addr); err == nil {
		t.Fatal("a rejected metadata call must surface the controller error")
	}
}

func TestAdminGetInSyncStateDataRoutesThroughLeader(t *testing.T) {
	c := newAdminCluster(t)
	replicasBody := []byte(`{"replicasInfoTable":{"b1":{` +
		`"masterBrokerId":0,"masterAddress":"10.0.0.1:30911","masterEpoch":1,"syncStateSetEpoch":2,` +
		`"inSyncReplicas":[{"brokerName":"b1","brokerId":0,"brokerAddress":"10.0.0.1:30911","alive":true}],` +
		`"notInSyncReplicas":[]}}}`)
	leaderSrv, leaderLog := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqControllerGetSyncStateData {
			return okCode(remoting.RespRequestCodeNotSupported, "sync state only")
		}
		return respOK(replicasBody)
	})
	metaSrv, metaLog := newRecordingServer(t, newControllerMetaAnswer(leaderSrv.addr))

	info, err := c.admin.GetInSyncStateData(metaSrv.addr, []string{"b1", "b2"})
	requireNoError(t, "getInSyncStateData", err)
	entry := info.ReplicasInfoTable["b1"]
	if entry == nil || entry.MasterAddress != "10.0.0.1:30911" ||
		entry.MasterBrokerID == nil || *entry.MasterBrokerID != 0 ||
		entry.SyncStateSetEpoch == nil || *entry.SyncStateSetEpoch != 2 ||
		len(entry.InSyncReplicas) != 1 || entry.InSyncReplicas[0].Alive == nil || !*entry.InSyncReplicas[0].Alive {
		t.Fatalf("replicas info = %+v", entry)
	}

	// The follow-up request carries the raw broker-name array in the body,
	// goes to the LEADER, and has no header of its own.
	req := leaderLog.first(remoting.ReqControllerGetSyncStateData)
	if req == nil {
		t.Fatal("the sync-state request never reached the leader")
	}
	if got := string(req.Body); got != `["b1","b2"]` {
		t.Errorf("sync-state body = %q, want the raw name array", got)
	}
	if req.ExtFields().Len() != 0 {
		t.Errorf("sync-state request must have no extFields, got %v", req.ExtFields().Keys())
	}
	if metaLog.count(remoting.ReqControllerGetSyncStateData) != 0 {
		t.Error("the metadata server must not see the sync-state request")
	}

	leaderless, leaderlessLog := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		resp := respOK(nil)
		resp.AddExtField("group", "raft-group")
		resp.AddExtField("controllerLeaderId", "n1")
		resp.AddExtField("isLeader", "false")
		return resp
	})
	_ = leaderlessLog
	if _, err := c.admin.GetInSyncStateData(leaderless.addr, []string{"b1"}); err == nil {
		t.Fatal("an answer without a leader address must fail the two-phase call")
	}
}

func TestAdminCleanControllerBrokerDataWireShape(t *testing.T) {
	c := newAdminCluster(t)
	leaderSrv, leaderLog := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqCleanBrokerData {
			return okCode(remoting.RespRequestCodeNotSupported, "clean only")
		}
		return respOK(nil)
	})
	metaSrv, metaLog := newRecordingServer(t, newControllerMetaAnswer(leaderSrv.addr))

	requireNoError(t, "cleanControllerBrokerData",
		c.admin.CleanControllerBrokerData(metaSrv.addr, "", "b1", "", false))
	req := leaderLog.first(remoting.ReqCleanBrokerData)
	if req == nil {
		t.Fatal("the clean request never reached the leader")
	}
	if got := ext(t, req, "brokerName"); got != "b1" {
		t.Errorf("brokerName = %q", got)
	}
	if got := ext(t, req, "isCleanLivingBroker"); got != "false" {
		t.Errorf("isCleanLivingBroker = %q, want false (the primitive default is sent)", got)
	}
	invokeTime, ok := adminParseInt64(ext(t, req, "invokeTime"))
	if !ok || invokeTime <= 0 {
		t.Errorf("invokeTime = %q, want a present timestamp", ext(t, req, "invokeTime"))
	}
	if hasExt(req, "clusterName") {
		t.Error("an empty clusterName must be omitted")
	}
	if hasExt(req, "brokerControllerIdsToClean") {
		t.Error("an empty brokerControllerIdsToClean must be omitted")
	}
	if metaLog.count(remoting.ReqCleanBrokerData) != 0 {
		t.Error("the clean request must go to the leader, not the metadata server")
	}

	// Second round: the optional fields are sent when set.
	requireNoError(t, "cleanControllerBrokerData(optional)",
		c.admin.CleanControllerBrokerData(metaSrv.addr, "DefaultCluster", "b1", "1;2", true))
	all := leaderLog.all(remoting.ReqCleanBrokerData)
	if len(all) != 2 {
		t.Fatalf("clean requests = %d, want 2", len(all))
	}
	second := all[1]
	for key, want := range map[string]string{
		"clusterName": "DefaultCluster", "brokerControllerIdsToClean": "1;2",
		"isCleanLivingBroker": "true",
	} {
		if got := ext(t, second, key); got != want {
			t.Errorf("clean %s = %q, want %q", key, got, want)
		}
	}
}

// ---------------- controller configuration ----------------

func TestAdminGetControllerConfigFansOverControllersAndParsesText(t *testing.T) {
	c := newAdminCluster(t)
	text := []byte("controllerConfigA=v\ncontrollerConfigB=w\n")
	ctrl1, log1 := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(text)
	})
	ctrl2, log2 := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(text)
	})

	configs, err := c.admin.GetControllerConfig([]string{ctrl1.addr, ctrl2.addr}, 0)
	requireNoError(t, "getControllerConfig", err)
	for _, addr := range []string{ctrl1.addr, ctrl2.addr} {
		got := configs[addr]
		if got == nil || got["controllerConfigA"] != "v" || got["controllerConfigB"] != "w" {
			t.Fatalf("config[%s] = %v", addr, got)
		}
	}
	if log1.count(remoting.ReqGetControllerConfig) != 1 || log2.count(remoting.ReqGetControllerConfig) != 1 {
		t.Fatalf("requests: ctrl1=%d ctrl2=%d, want 1 each",
			log1.count(remoting.ReqGetControllerConfig), log2.count(remoting.ReqGetControllerConfig))
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "nope")
	})
	if _, err := c.admin.GetControllerConfig([]string{ctrl1.addr, rejected.addr}, 0); err == nil ||
		!strings.Contains(err.Error(), "nope") {
		t.Fatalf("one rejecting controller must fail the read with its remark, got %v", err)
	}

	// With no controller list the NAMESERVERS are asked.
	c.nsAnswer[remoting.ReqGetControllerConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte("nsKey=nsVal\n"))
	}
	fromNS, err := c.admin.GetControllerConfig(nil, 0)
	requireNoError(t, "getControllerConfig(fallback)", err)
	if fromNS[c.nsSrv.addr]["nsKey"] != "nsVal" {
		t.Fatalf("fallback config = %v", fromNS)
	}
}

func TestAdminUpdateControllerConfigSendsTextAndKeepsLastError(t *testing.T) {
	c := newAdminCluster(t)
	first, firstLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "first rejection")
	})
	second, secondLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "second rejection")
	})
	healthy, healthyLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})

	// The LAST rejection wins, even when a later controller accepted.
	err := c.admin.UpdateControllerConfig(map[string]string{"k": "v"},
		[]string{first.addr, second.addr, healthy.addr}, 0)
	if err == nil || !strings.Contains(err.Error(), "second rejection") {
		t.Fatalf("the last rejection must surface, got %v", err)
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Fatalf("error must carry the controller code, got %v", err)
	}
	// Every controller is still visited.
	for _, log := range []*reqLog{firstLog, secondLog, healthyLog} {
		if got := log.count(remoting.ReqUpdateControllerConfig); got != 1 {
			t.Fatalf("UPDATE_CONTROLLER_CONFIG sent %d times, want 1", got)
		}
	}
	req := healthyLog.first(remoting.ReqUpdateControllerConfig)
	if got := string(req.Body); got != "k=v\n" {
		t.Errorf("body = %q, want properties text", got)
	}
	if req.ExtFields().Len() != 0 {
		t.Errorf("extField sent: %v — the payload is the body", req.ExtFields().Keys())
	}

	// Empty properties or an empty controller list never leave the client.
	requireNoError(t, "updateControllerConfig(empty props)",
		c.admin.UpdateControllerConfig(map[string]string{}, []string{healthy.addr}, 0))
	requireNoError(t, "updateControllerConfig(no controllers)",
		c.admin.UpdateControllerConfig(map[string]string{"k": "v"}, nil, 0))
	if healthyLog.count(remoting.ReqUpdateControllerConfig) != 1 {
		t.Fatalf("no-op runs sent requests: %d", healthyLog.count(remoting.ReqUpdateControllerConfig)-1)
	}
}

// ---------------- cold data flow control ----------------

func TestAdminUpdateColdDataFlowCtrConfigSendsPropertiesTextThroughVIP(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqUpdateColdDataFlowCtrConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	c.admin.SetVipChannelEnabled(true)
	// Publish the +2 address: the request only arrives when the VIP rewrite
	// IS applied.
	published := shiftPort(t, c.brokerSrv.addr, 2)
	requireNoError(t, "updateColdDataFlowCtrConfig",
		c.admin.UpdateColdDataFlowCtrConfig(published,
			map[string]string{"b": "2", "consumerGroupA": "1"}, 0))

	req := c.brokerLog.first(remoting.ReqUpdateColdDataFlowCtrConfig)
	if req == nil {
		t.Fatal("no UPDATE_COLD_DATA_FLOW_CTR_CONFIG reached the broker")
	}
	// Line order is not part of the contract: the broker reads the properties
	// text back by name, so assert the exact lines, not their order.
	if got := string(req.Body); got != "b=2\nconsumerGroupA=1\n" && got != "consumerGroupA=1\nb=2\n" {
		t.Errorf("body = %q, want the properties text lines b=2 and consumerGroupA=1", got)
	}
	if req.ExtFields().Len() != 0 {
		t.Errorf("extField sent: %v — the payload is the body", req.ExtFields().Keys())
	}

	before := c.brokerLog.count(remoting.ReqUpdateColdDataFlowCtrConfig)
	requireNoError(t, "updateColdDataFlowCtrConfig(empty)",
		c.admin.UpdateColdDataFlowCtrConfig(published, map[string]string{}, 0))
	if got := c.brokerLog.count(remoting.ReqUpdateColdDataFlowCtrConfig); got != before {
		t.Fatalf("an empty property set must skip the RPC, sent %d extra", got-before)
	}
}

func TestAdminRemoveColdDataFlowCtrConfigSendsRawGroupBody(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqRemoveColdDataFlowCtrConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	c.admin.SetVipChannelEnabled(true)
	published := shiftPort(t, c.brokerSrv.addr, 2)
	requireNoError(t, "removeColdDataFlowCtrConfig",
		c.admin.RemoveColdDataFlowCtrConfig(published, "G1", 0))

	req := c.brokerLog.first(remoting.ReqRemoveColdDataFlowCtrConfig)
	if req == nil {
		t.Fatal("no REMOVE_COLD_DATA_FLOW_CTR_CONFIG reached the broker")
	}
	if got := string(req.Body); got != "G1" {
		// A JSON-wrapped body (`"G1"`) is the classic wrong shape here.
		t.Errorf("body = %q, want the raw group string", got)
	}
	if req.ExtFields().Len() != 0 {
		t.Errorf("extField sent: %v", req.ExtFields().Keys())
	}

	before := c.brokerLog.count(remoting.ReqRemoveColdDataFlowCtrConfig)
	requireNoError(t, "removeColdDataFlowCtrConfig(empty)",
		c.admin.RemoveColdDataFlowCtrConfig(published, "", 0))
	if got := c.brokerLog.count(remoting.ReqRemoveColdDataFlowCtrConfig); got != before {
		t.Fatalf("an empty group must skip the RPC, sent %d extra", got-before)
	}
}

func TestAdminGetColdDataFlowCtrInfoStaysOnPlainPort(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetColdDataFlowCtrInfo] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte("cmOff_group=1:100\n"))
	}
	// The VIP switch is on and the PLAIN address is called: this RPC must not
	// move to port-2.
	c.admin.SetVipChannelEnabled(true)
	info, err := c.admin.GetColdDataFlowCtrInfo(c.brokerSrv.addr, 0)
	requireNoError(t, "getColdDataFlowCtrInfo", err)
	if info != "cmOff_group=1:100\n" {
		t.Errorf("info = %q, want the raw body text", info)
	}
	req := c.brokerLog.first(remoting.ReqGetColdDataFlowCtrInfo)
	if req.ExtFields().Len() != 0 || len(req.Body) != 0 {
		t.Errorf("request carries ext %v / body %q", req.ExtFields().Keys(), string(req.Body))
	}

	c.brokerAnswer[remoting.ReqGetColdDataFlowCtrInfo] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	empty, err := c.admin.GetColdDataFlowCtrInfo(c.brokerSrv.addr, 0)
	requireNoError(t, "getColdDataFlowCtrInfo(empty)", err)
	if empty != "" {
		t.Errorf("empty body must read as an empty string, got %q", empty)
	}

	c.brokerAnswer[remoting.ReqGetColdDataFlowCtrInfo] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "cold data disabled")
	}
	if _, err := c.admin.GetColdDataFlowCtrInfo(c.brokerSrv.addr, 0); err == nil ||
		!strings.Contains(err.Error(), "cold data disabled") {
		t.Fatalf("a rejection must surface, got %v", err)
	}
}

// ---------------- commit-log read ahead ----------------

func TestAdminSetCommitLogReadAheadModeSingleExtField(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqSetCommitLogReadMode] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSuccess, "normal")
	}
	c.admin.SetVipChannelEnabled(true)
	remark, err := c.admin.SetCommitLogReadAheadMode(c.brokerSrv.addr, "SERVER", 0)
	requireNoError(t, "setCommitLogReadAheadMode", err)
	if remark != "normal" {
		t.Errorf("remark = %q, want the broker's remark", remark)
	}
	req := c.brokerLog.first(remoting.ReqSetCommitLogReadMode)
	if req == nil {
		t.Fatal("no SET_COMMITLOG_READ_MODE request")
	}
	if got := ext(t, req, "READ_AHEAD_MODE"); got != "SERVER" {
		t.Errorf("READ_AHEAD_MODE = %q", got)
	}
	if req.ExtFields().Len() != 1 {
		t.Errorf("extFields = %v, want only READ_AHEAD_MODE", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("body = %q, the mode rides in the header", string(req.Body))
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "unknown mode")
	})
	if _, err := c.admin.SetCommitLogReadAheadMode(rejected.addr, "WRONG", 0); err == nil ||
		!strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("a rejection must surface, got %v", err)
	}
}

// ---------------- pop record export ----------------

func TestAdminExportPopRecordsStaysOnPlainPort(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqPopRollback] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	c.admin.SetVipChannelEnabled(true)
	// The POP rollback is served on the listening port even with the VIP
	// switch on.
	requireNoError(t, "exportPopRecords", c.admin.ExportPopRecords(c.brokerSrv.addr, 3000))
	req := c.brokerLog.first(remoting.ReqPopRollback)
	if req == nil {
		t.Fatal("no POP_ROLLBACK request")
	}
	if req.ExtFields().Len() != 0 || len(req.Body) != 0 {
		t.Errorf("request carries ext %v / body %q", req.ExtFields().Keys(), string(req.Body))
	}

	c.brokerAnswer[remoting.ReqPopRollback] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "export failed")
	}
	if err := c.admin.ExportPopRecords(c.brokerSrv.addr, 3000); err == nil ||
		!strings.Contains(err.Error(), "export failed") {
		t.Fatalf("a rejection must surface, got %v", err)
	}
}

// ---------------- RocksDB consume queue ----------------

func TestAdminCheckRocksdbCqWriteProgressAlwaysSendsCheckStoreTime(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqCheckRocksdbCqWriteProgress] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"checkResult":"lag 3","checkStatus":2}`))
	}
	// No VIP: the check is served on the listening port.
	c.admin.SetVipChannelEnabled(true)
	result, err := c.admin.CheckRocksdbCqWriteProgress(c.brokerSrv.addr, "MyTopic", 1700000000000, 0)
	requireNoError(t, "checkRocksdbCqWriteProgress", err)
	if result.CheckResult != "lag 3" || result.CheckStatus != remoting.RocksdbCheckInProgress {
		t.Fatalf("result = %+v", result)
	}
	req := c.brokerLog.first(remoting.ReqCheckRocksdbCqWriteProgress)
	if req == nil {
		t.Fatal("no CHECK_ROCKSDB_CQ_WRITE_PROGRESS request")
	}
	if got := ext(t, req, "topic"); got != "MyTopic" {
		t.Errorf("topic = %q", got)
	}
	// checkStoreTime is a primitive: it is sent even for zero.
	if got := ext(t, req, "checkStoreTime"); got != "1700000000000" {
		t.Errorf("checkStoreTime = %q", got)
	}
	if hasExt(req, "checkStoreTime") == false || req.ExtFields().Len() != 2 {
		t.Errorf("extFields = %v, want exactly topic + checkStoreTime", req.ExtFields().Keys())
	}

	c.brokerAnswer[remoting.ReqCheckRocksdbCqWriteProgress] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "rocksdb cq not enabled")
	}
	if _, err := c.admin.CheckRocksdbCqWriteProgress(c.brokerSrv.addr, "MyTopic", 0, 0); err == nil ||
		!strings.Contains(err.Error(), "rocksdb cq not enabled") {
		t.Fatalf("a rejection must surface, got %v", err)
	}
}

func TestAdminExportRocksDBConfigToJsonJoinsConfigTypes(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqExportRocksdbConfigToJSON] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	requireNoError(t, "exportRocksDBConfigToJson",
		c.admin.ExportRocksDBConfigToJson(c.brokerSrv.addr,
			[]string{remoting.RocksDBConfigTopics, remoting.RocksDBConfigSubscriptionGroup, remoting.RocksDBConfigConsumerOffsets}, 0))
	req := c.brokerLog.first(remoting.ReqExportRocksdbConfigToJSON)
	if req == nil {
		t.Fatal("no EXPORT_ROCKSDB_CONFIG_TO_JSON request")
	}
	// The separator is part of the protocol value: the trailing ';' tells the
	// broker the list ended.
	if got := ext(t, req, "configType"); got != "topics;subscriptionGroups;consumerOffsets;" {
		t.Errorf("configType = %q, want the ';'-joined value with the trailing separator", got)
	}
	if req.ExtFields().Len() != 1 {
		t.Errorf("extFields = %v, want only configType", req.ExtFields().Keys())
	}

	requireNoError(t, "exportRocksDBConfigToJson(subset)",
		c.admin.ExportRocksDBConfigToJson(c.brokerSrv.addr, []string{remoting.RocksDBConfigTopics}, 0))
	second := c.brokerLog.all(remoting.ReqExportRocksdbConfigToJSON)[1]
	if got := ext(t, second, "configType"); got != "topics;" {
		t.Errorf("configType = %q", got)
	}

	if err := c.admin.ExportRocksDBConfigToJson(c.brokerSrv.addr, nil, 0); err == nil {
		t.Fatal("an empty type list must be rejected client-side")
	}
	if got := c.brokerLog.count(remoting.ReqExportRocksdbConfigToJSON); got != 2 {
		t.Fatalf("requests = %d, want 2 (the empty list sent nothing)", got)
	}
}
