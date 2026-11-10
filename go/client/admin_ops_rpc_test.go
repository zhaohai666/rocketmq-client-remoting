// Wire-shape tests for the #47 mqadmin ops RPC family: 308 / 311 / 312 / 313 /
// 328 / 907 / 908 / 1002 / 1007 / 5001. Every call runs against the in-process
// mock cluster; no real broker, name server or controller is involved.
package client

import (
	"math"
	"strconv"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const opsBrokerAddr = "127.0.0.1:10911"

// ------------------------------------------------------------------- 308

func TestAdminQueryCorrectionOffsetWireShape(t *testing.T) {
	c := newAdminCluster(t)
	body := []byte(`{"correctionOffsets":{"0":110,"3":407}}`)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(body)
	})

	offsets, err := c.admin.QueryCorrectionOffset(srv.addr, "OpsTopic", "GID_Ops",
		[]string{"GID_other", "GID_x"}, 5000)
	requireNoError(t, "queryCorrectionOffset", err)
	if len(offsets) != 2 || offsets[0] != 110 || offsets[3] != 407 {
		t.Fatalf("correctionOffsets = %v", offsets)
	}

	req := log.first(remoting.ReqQueryCorrectionOffset)
	if req == nil {
		t.Fatal("no 308 reached the broker")
	}
	// Java joins the Set itself, comma-joined; compareGroup carries the group.
	if got := ext(t, req, "topic"); got != "OpsTopic" {
		t.Errorf("topic = %q", got)
	}
	if got := ext(t, req, "compareGroup"); got != "GID_Ops" {
		t.Errorf("compareGroup = %q", got)
	}
	got := ext(t, req, "filterGroups")
	if got != "GID_other,GID_x" && got != "GID_x,GID_other" {
		t.Errorf("filterGroups = %q, want the comma-joined set", got)
	}
	if req.ExtFields().Len() != 3 {
		t.Errorf("extFields = %v, want exactly the three header fields", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("308 must not carry a request body, got %q", string(req.Body))
	}
}

func TestAdminQueryCorrectionOffsetOmitsEmptyFilterGroups(t *testing.T) {
	c := newAdminCluster(t)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"correctionOffsets":{}}`))
	})

	if _, err := c.admin.QueryCorrectionOffset(srv.addr, "OpsTopic", "GID_Ops", nil, 5000); err != nil {
		t.Fatalf("queryCorrectionOffset: %v", err)
	}
	req := log.first(remoting.ReqQueryCorrectionOffset)
	if req == nil {
		t.Fatal("no 308 reached the broker")
	}
	if hasExt(req, "filterGroups") {
		t.Errorf("filterGroups = %q, want the key omitted", ext(t, req, "filterGroups"))
	}
	if req.ExtFields().Len() != 2 {
		t.Errorf("extFields = %v, want topic + compareGroup only", req.ExtFields().Keys())
	}
}

func TestAdminQueryCorrectionOffsetVIPBothStates(t *testing.T) {
	c := newAdminCluster(t)
	// VIP off: the published address answers as-is.
	plain, plainLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"correctionOffsets":{}}`))
	})
	if _, err := c.admin.QueryCorrectionOffset(plain.addr, "T", "G", nil, 5000); err != nil {
		t.Fatalf("vip off: %v", err)
	}
	if got := plainLog.count(remoting.ReqQueryCorrectionOffset); got != 1 {
		t.Fatalf("vip off landed %d times, want 1", got)
	}

	// VIP on: handing the address two ports above the listener proves the
	// request was rewritten down by 2 (Java brokerVIPChannel).
	c.admin.SetVipChannelEnabled(true)
	vip, vipLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"correctionOffsets":{}}`))
	})
	shifted := shiftPort(t, vip.addr, 2)
	if _, err := c.admin.QueryCorrectionOffset(shifted, "T", "G", nil, 5000); err != nil {
		t.Fatalf("vip on: %v", err)
	}
	if got := vipLog.count(remoting.ReqQueryCorrectionOffset); got != 1 {
		t.Fatalf("vip on landed %d times on the shifted port, want 1", got)
	}
}

// ------------------------------------------------------------------- 311/312/313

func TestAdminUnitTopicListFamilyHitsOneNameServerWithNullHeader(t *testing.T) {
	c := newAdminCluster(t)
	body := []byte(`{"topicList":["TopicA","%RETRY%G"]}`)
	cases := []struct {
		code int32
		call func(bool, int64) (any, error)
	}{
		{remoting.ReqGetUnitTopicList, func(cr bool, ms int64) (any, error) {
			return c.admin.GetUnitTopicList(cr, ms)
		}},
		{remoting.ReqGetHasUnitSubTopicList, func(cr bool, ms int64) (any, error) {
			return c.admin.GetHasUnitSubTopicList(cr, ms)
		}},
		{remoting.ReqGetHasUnitSubUnUnitTopicList, func(cr bool, ms int64) (any, error) {
			return c.admin.GetHasUnitSubUnUnitTopicList(cr, ms)
		}},
	}
	for _, tc := range cases {
		c.nsAnswer[tc.code] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
			return respOK(body)
		}
		topics, err := tc.call(true, 5000)
		requireNoError(t, "unit topic list", err)
		tl := topics.(*remoting.TopicList)
		if len(tl.TopicList) != 2 || tl.TopicList[0] != "TopicA" || tl.TopicList[1] != "%RETRY%G" {
			t.Fatalf("code %d topicList = %v", tc.code, tl.TopicList)
		}
		// Java invokeSync(null, ...) picks a name server: the request must have
		// landed on the name server, never on the broker.
		req := c.nsLog.first(tc.code)
		if req == nil {
			t.Fatalf("code %d never reached the name server", tc.code)
		}
		if req.ExtFields().Len() != 0 {
			t.Errorf("code %d extFields = %v, want a null header", tc.code, req.ExtFields().Keys())
		}
		if len(req.Body) != 0 {
			t.Errorf("code %d must not carry a body", tc.code)
		}
		if got := c.brokerLog.count(tc.code); got != 0 {
			t.Errorf("code %d reached the broker %d times", tc.code, got)
		}
		delete(c.nsAnswer, tc.code)
		c.nsLog = &reqLog{}
	}
}

func TestAdminUnitTopicListFiltersRetryTopicsLocally(t *testing.T) {
	c := newAdminCluster(t)
	c.nsAnswer[remoting.ReqGetUnitTopicList] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"topicList":["TopicA","%RETRY%G","%RETRY%G_lite"]}`))
	}

	topics, err := c.admin.GetUnitTopicList(false, 5000)
	requireNoError(t, "getUnitTopicList", err)
	if len(topics.TopicList) != 1 || topics.TopicList[0] != "TopicA" {
		t.Fatalf("filtered topicList = %v, want [TopicA]", topics.TopicList)
	}
	// The raw answer is untouched on the wire: the filter happens after decoding.
	req := c.nsLog.first(remoting.ReqGetUnitTopicList)
	if req == nil || req.ExtFields().Len() != 0 {
		t.Fatalf("the request itself must stay a null header, got %v", req)
	}
}

func TestAdminUnitTopicListFailureCarriesTheCode(t *testing.T) {
	c := newAdminCluster(t)
	c.nsAnswer[remoting.ReqGetHasUnitSubTopicList] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "boom")
	}
	if _, err := c.admin.GetHasUnitSubTopicList(true, 5000); err == nil {
		t.Fatal("a rejected 312 must surface the name server error")
	} else if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Fatalf("error must carry the name server code, got %v", err)
	}
}

// ------------------------------------------------------------------- 328

func TestAdminGetAllProducerInfoWireShape(t *testing.T) {
	c := newAdminCluster(t)
	body := []byte(`{"data":{"G":[{"clientId":"cid-1","remoteIP":"10.0.0.1",` +
		`"language":"JAVA","version":500,"lastUpdateTimestamp":1700000000000}]}}`)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(body)
	})

	table, err := c.admin.GetAllProducerInfo(srv.addr, 5000)
	requireNoError(t, "getAllProducerInfo", err)
	list := table.Data["G"]
	if len(list) != 1 {
		t.Fatalf("producer table = %+v", table.Data)
	}
	info := list[0]
	if info.ClientID != "cid-1" || info.RemoteIP != "10.0.0.1" || info.Language != "JAVA" ||
		info.Version != 500 || info.LastUpdateTimestamp != 1700000000000 {
		t.Fatalf("producer info = %+v", info)
	}

	req := log.first(remoting.ReqGetAllProducerInfo)
	if req == nil {
		t.Fatal("no 328 reached the broker")
	}
	// GetAllProducerInfoRequestHeader is empty: the extFields stay empty.
	if req.ExtFields().Len() != 0 {
		t.Errorf("extFields = %v, want empty", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("328 must not carry a request body")
	}
}

func TestAdminGetAllProducerInfoVIPBothStates(t *testing.T) {
	c := newAdminCluster(t)
	plain, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"data":{}}`))
	})
	if _, err := c.admin.GetAllProducerInfo(plain.addr, 5000); err != nil {
		t.Fatalf("vip off: %v", err)
	}
	c.admin.SetVipChannelEnabled(true)
	vip, vipLog := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`{"data":{}}`))
	})
	if _, err := c.admin.GetAllProducerInfo(shiftPort(t, vip.addr, 2), 5000); err != nil {
		t.Fatalf("vip on: %v", err)
	}
	if got := vipLog.count(remoting.ReqGetAllProducerInfo); got != 1 {
		t.Fatalf("vip on landed %d times on the shifted port, want 1", got)
	}
}

// ------------------------------------------------------------------- 907

func TestAdminGetBrokerHAStatusDecodesNestedRuntimeInfo(t *testing.T) {
	c := newAdminCluster(t)
	body := []byte(`{"master":true,"masterCommitLogMaxOffset":9001,"inSyncSlaveNums":2,` +
		`"haConnectionInfo":[{"addr":"10.0.0.2:10912","slaveAckOffset":8900,"diff":101,` +
		`"inSync":true,"transferredByteInSecond":2048,"transferFromWhere":8700}],` +
		`"haClientRuntimeInfo":{"masterAddr":"10.0.0.1:10912","transferredByteInSecond":12,` +
		`"maxOffset":9001,"lastReadTimestamp":111,"lastWriteTimestamp":222,` +
		`"masterFlushOffset":8800,"activated":true}}`)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(body)
	})

	info, err := c.admin.GetBrokerHAStatus(srv.addr, 5000)
	requireNoError(t, "getBrokerHAStatus", err)
	if !info.Master || info.MasterCommitLogMaxOffset != 9001 || info.InSyncSlaveNums != 2 {
		t.Fatalf("ha runtime = %+v", info)
	}
	if len(info.HAConnectionInfo) != 1 {
		t.Fatalf("haConnectionInfo = %+v", info.HAConnectionInfo)
	}
	conn := info.HAConnectionInfo[0]
	if conn.Addr != "10.0.0.2:10912" || conn.SlaveAckOffset != 8900 || conn.Diff != 101 ||
		!conn.InSync || conn.TransferredByteInSecond != 2048 || conn.TransferFromWhere != 8700 {
		t.Fatalf("connection info = %+v", conn)
	}
	hc := info.HAClientRuntimeInfo
	if hc == nil || hc.MasterAddr != "10.0.0.1:10912" || hc.MaxOffset != 9001 ||
		hc.MasterFlushOffset != 8800 || !hc.Activated {
		t.Fatalf("ha client info = %+v", hc)
	}

	req := log.first(remoting.ReqGetBrokerHAStatus)
	if req == nil {
		t.Fatal("no 907 reached the broker")
	}
	// Plain broker address family: no header fields, no request body.
	if req.ExtFields().Len() != 0 {
		t.Errorf("extFields = %v, want empty", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("907 must not carry a request body")
	}
}

// ------------------------------------------------------------------- 908

func TestAdminResetMasterFlushOffsetWireShape(t *testing.T) {
	c := newAdminCluster(t)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})

	requireNoError(t, "resetMasterFlushOffset",
		c.admin.ResetMasterFlushOffset(srv.addr, 12345))

	req := log.first(remoting.ReqResetMasterFlushOffset)
	if req == nil {
		t.Fatal("no 908 reached the broker")
	}
	if req.ExtFields().Len() != 1 {
		t.Fatalf("extFields = %v, want only masterFlushOffset", req.ExtFields().Keys())
	}
	if got := ext(t, req, "masterFlushOffset"); got != "12345" {
		t.Errorf("masterFlushOffset = %q, want the primitive long text", got)
	}
	if len(req.Body) != 0 {
		t.Errorf("908 must not carry a request body")
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "not master")
	})
	err := c.admin.ResetMasterFlushOffset(rejected.addr, 12345)
	if err == nil {
		t.Fatal("a rejected 908 must surface the broker error")
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Fatalf("error must carry the broker code, got %v", err)
	}
}

// ------------------------------------------------------------------- 1002

func newOpsControllerAnswer(leaderAddr string) func(*remoting.RemotingCommand) *remoting.RemotingCommand {
	return newControllerMetaAnswer(leaderAddr)
}

func TestAdminElectMasterTwoPhase(t *testing.T) {
	c := newAdminCluster(t)
	memberBody := []byte(`{"cluster":"DefaultCluster","brokerName":"broker-a",` +
		`"brokerAddrs":{"0":"10.0.0.1:10911","1":"10.0.0.2:10911"}}`)
	leaderSrv, leaderLog := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqControllerElectMaster {
			return okCode(remoting.RespRequestCodeNotSupported, "elect only")
		}
		resp := respOK(memberBody)
		resp.AddExtField("masterBrokerId", "1")
		resp.AddExtField("masterAddress", "10.0.0.2:10911")
		resp.AddExtField("masterEpoch", "3")
		resp.AddExtField("syncStateSetEpoch", "7")
		return resp
	})
	metaSrv, metaLog := newRecordingServer(t, newOpsControllerAnswer(leaderSrv.addr))

	header, group, err := c.admin.ElectMaster(metaSrv.addr, "DefaultCluster", "broker-a", -1)
	requireNoError(t, "electMaster", err)

	// Step 1: the 1005 metadata read names the leader.
	if got := metaLog.count(remoting.ReqControllerGetMetadataInfo); got != 1 {
		t.Fatalf("metadata requests = %d, want 1", got)
	}
	// Step 2: the 1002 admin trigger goes to the LEADER only.
	req := leaderLog.first(remoting.ReqControllerElectMaster)
	if req == nil {
		t.Fatal("the elect request never reached the leader")
	}
	if got := metaLog.count(remoting.ReqControllerElectMaster); got != 0 {
		t.Errorf("the metadata server must not see the elect request")
	}
	for key, want := range map[string]string{
		"clusterName":    "DefaultCluster",
		"brokerName":     "broker-a",
		"brokerId":       "-1",
		"designateElect": "true",
	} {
		if got := ext(t, req, key); got != want {
			t.Errorf("elect %s = %q, want %q", key, got, want)
		}
	}
	invokeTime, err := strconv.ParseInt(ext(t, req, "invokeTime"), 10, 64)
	if err != nil || invokeTime <= 0 {
		t.Errorf("invokeTime = %q, want the current millis", ext(t, req, "invokeTime"))
	}
	if len(req.Body) != 0 {
		t.Errorf("1002 must not carry a request body")
	}

	if header.MasterBrokerID == nil || *header.MasterBrokerID != 1 ||
		header.MasterAddress == nil || *header.MasterAddress != "10.0.0.2:10911" ||
		header.MasterEpoch == nil || *header.MasterEpoch != 3 ||
		header.SyncStateSetEpoch == nil || *header.SyncStateSetEpoch != 7 {
		t.Fatalf("elect header = %+v", header)
	}
	if group == nil || group.Cluster != "DefaultCluster" || group.BrokerName != "broker-a" ||
		group.BrokerAddrs[0] != "10.0.0.1:10911" || group.BrokerAddrs[1] != "10.0.0.2:10911" {
		t.Fatalf("member group = %+v", group)
	}

	leaderless, _ := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		resp := respOK(nil)
		resp.AddExtField("group", "raft-group")
		resp.AddExtField("isLeader", "false")
		return resp
	})
	if _, _, err := c.admin.ElectMaster(leaderless.addr, "DefaultCluster", "broker-a", -1); err == nil {
		t.Fatal("an answer without a leader address must fail the two-phase call")
	}
}

func TestAdminElectMasterSendsDesignatedBrokerID(t *testing.T) {
	c := newAdminCluster(t)
	leaderSrv, leaderLog := newRecordingServer(t, func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqControllerElectMaster {
			return okCode(remoting.RespRequestCodeNotSupported, "elect only")
		}
		return respOK(nil)
	})
	metaSrv, _ := newRecordingServer(t, newOpsControllerAnswer(leaderSrv.addr))

	if _, _, err := c.admin.ElectMaster(metaSrv.addr, "DefaultCluster", "broker-a", 1); err != nil {
		t.Fatalf("electMaster: %v", err)
	}
	if got := ext(t, leaderLog.first(remoting.ReqControllerElectMaster), "brokerId"); got != "1" {
		t.Errorf("brokerId = %q, want the designated id", got)
	}
}

// ------------------------------------------------------------------- 1007

func TestAdminGetBrokerEpochCacheDecodesWithDefaultEndOffset(t *testing.T) {
	c := newAdminCluster(t)
	body := []byte(`{"clusterName":"DefaultCluster","brokerName":"broker-a","brokerId":1,` +
		`"epochList":[{"epoch":1,"startOffset":0,"endOffset":500},{"epoch":2,"startOffset":500}],` +
		`"maxOffset":900}`)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(body)
	})

	cache, err := c.admin.GetBrokerEpochCache(srv.addr)
	requireNoError(t, "getBrokerEpochCache", err)
	if cache.ClusterName != "DefaultCluster" || cache.BrokerName != "broker-a" ||
		cache.BrokerID != 1 || cache.MaxOffset != 900 {
		t.Fatalf("epoch cache = %+v", cache)
	}
	if len(cache.EpochList) != 2 {
		t.Fatalf("epochList = %+v", cache.EpochList)
	}
	first, second := cache.EpochList[0], cache.EpochList[1]
	if first.Epoch != 1 || first.StartOffset != 0 || first.EndOffset != 500 {
		t.Fatalf("first epoch = %+v", first)
	}
	// A live epoch omits endOffset on the wire: the decoder fills in Long.MAX.
	if second.Epoch != 2 || second.StartOffset != 500 ||
		second.EndOffset != int64(math.MaxInt64) {
		t.Fatalf("second epoch = %+v, want endOffset 9223372036854775807", second)
	}

	req := log.first(remoting.ReqGetBrokerEpochCache)
	if req == nil {
		t.Fatal("no 1007 reached the broker")
	}
	if req.ExtFields().Len() != 0 {
		t.Errorf("extFields = %v, want empty", req.ExtFields().Keys())
	}
	if len(req.Body) != 0 {
		t.Errorf("1007 must not carry a request body")
	}
}

// ------------------------------------------------------------------- 5001

func TestAdminSwitchTimerEngineWireShape(t *testing.T) {
	c := newAdminCluster(t)
	srv, log := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})

	requireNoError(t, "switchTimerEngine",
		c.admin.SwitchTimerEngine(srv.addr, "rocksdb", 4500))

	req := log.first(remoting.ReqSwitchTimerEngine)
	if req == nil {
		t.Fatal("no 5001 reached the broker")
	}
	if req.ExtFields().Len() != 1 {
		t.Fatalf("extFields = %v, want only E_T", req.ExtFields().Keys())
	}
	if got := ext(t, req, "E_T"); got != "rocksdb" {
		t.Errorf("E_T = %q, want the engine type", got)
	}
	if len(req.Body) != 0 {
		t.Errorf("5001 must not carry a request body")
	}

	rejected, _ := newRecordingServer(t, func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "engine busy")
	})
	err := c.admin.SwitchTimerEngine(rejected.addr, "rocksdb", 4500)
	if err == nil {
		t.Fatal("a rejected 5001 must surface the broker error")
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Fatalf("error must carry the broker code, got %v", err)
	}
}
