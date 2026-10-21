// Metadata paging contracts for GET_ALL_TOPIC_CONFIG(21) and
// GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201).
//
// The cursor advances by the number of entries the broker RETURNED, not by the
// requested page size, and a changed dataVersion restarts the walk from seq 0
// with the new version on the very next request. Both rules have their own
// tests because getting either wrong silently truncates the table.
package client

import (
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// pagingVersion is the dataVersion shape used by these tests. A single string
// field keeps the encoded JSON deterministic.
func pagingVersion(id string) map[string]any {
	return map[string]any{"id": id}
}

func pagingVersionJSON(id string) string {
	return string(remoting.EncodeJSON(pagingVersion(id)))
}

// ---------------------------------------------------------------- topics

// A page shorter than maxTopicNum must advance the cursor by the entries the
// broker actually returned. Advancing by the requested size would stop after
// the first page and lose the tail of the table.
func TestAdminGetAllTopicConfigPaginatesByReturnedCount(t *testing.T) {
	c := newAdminCluster(t)
	c.admin.SetMaxPageSizeInGetMetadata(5)

	pages := map[string][]string{
		"0": {"T1", "T2"},
		"2": {"T3", "T4"},
	}
	c.brokerAnswer[remoting.ReqGetAllTopicConfig] = func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		seq, _ := req.ExtFields().Get("topicSeq")
		w := remoting.NewTopicConfigSerializeWrapper()
		for _, name := range pages[seq] {
			w.TopicConfigTable[name] = remoting.NewTopicConfig(name)
		}
		w.DataVersion = pagingVersion("v1")
		resp := respOK(w.Encode())
		resp.AddExtField("totalTopicNum", "4")
		return resp
	}

	wrapper, err := c.admin.GetAllTopicConfig(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllTopicConfig", err)

	reqs := c.brokerLog.all(remoting.ReqGetAllTopicConfig)
	if len(reqs) != 2 {
		t.Fatalf("pages requested = %d, want 2", len(reqs))
	}
	for k, want := range map[string]string{"topicSeq": "0", "maxTopicNum": "5"} {
		if got := ext(t, reqs[0], k); got != want {
			t.Errorf("first page %s = %q, want %q", k, got, want)
		}
	}
	if !hasExt(reqs[0], "dataVersion") {
		t.Error("the first request must carry dataVersion, present and empty")
	} else if got := ext(t, reqs[0], "dataVersion"); got != "" {
		t.Errorf("first page dataVersion = %q, want the empty string", got)
	}
	if got := ext(t, reqs[1], "topicSeq"); got != "2" {
		t.Errorf("second page topicSeq = %q, want 2 (returned entries, not maxTopicNum)", got)
	}
	if got := ext(t, reqs[1], "maxTopicNum"); got != "5" {
		t.Errorf("second page maxTopicNum = %q, want 5", got)
	}
	if got, want := ext(t, reqs[1], "dataVersion"), pagingVersionJSON("v1"); got != want {
		t.Errorf("second page dataVersion = %q, want %q", got, want)
	}

	for _, name := range []string{"T1", "T2", "T3", "T4"} {
		if wrapper.TopicConfigTable[name] == nil {
			t.Errorf("topic %s missing from the merged table: %v", name, keysOfTopicTable(wrapper))
		}
	}
	if len(wrapper.TopicConfigTable) != 4 {
		t.Errorf("merged topics = %d, want 4", len(wrapper.TopicConfigTable))
	}
	if got := string(remoting.EncodeJSON(wrapper.DataVersion)); got != pagingVersionJSON("v1") {
		t.Errorf("merged dataVersion = %q, want the first page's %q", got, pagingVersionJSON("v1"))
	}
}

// A version change mid-walk discards everything collected so far and restarts
// from seq 0. The restart request must carry the NEW version, never an empty
// one: an empty dataVersion asks the broker for a fresh enumeration instead of
// the version the walk is now following.
func TestAdminGetAllTopicConfigRestartsOnVersionChange(t *testing.T) {
	c := newAdminCluster(t)

	pages := []struct {
		version string
		names   []string
	}{
		{"v1", []string{"A", "B"}},
		{"v2", []string{"C"}},
		{"v2", []string{"A", "B"}},
		{"v2", []string{"C", "D"}},
	}
	var mu sync.Mutex
	next := 0
	c.brokerAnswer[remoting.ReqGetAllTopicConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		mu.Lock()
		page := pages[next]
		next++
		mu.Unlock()
		w := remoting.NewTopicConfigSerializeWrapper()
		for _, name := range page.names {
			w.TopicConfigTable[name] = remoting.NewTopicConfig(name)
		}
		w.DataVersion = pagingVersion(page.version)
		resp := respOK(w.Encode())
		resp.AddExtField("totalTopicNum", "4")
		return resp
	}

	wrapper, err := c.admin.GetAllTopicConfig(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllTopicConfig", err)

	reqs := c.brokerLog.all(remoting.ReqGetAllTopicConfig)
	if len(reqs) != 4 {
		t.Fatalf("pages requested = %d, want 4 (two pages before the change, two after)", len(reqs))
	}
	wantVersions := []string{"", pagingVersionJSON("v1"), pagingVersionJSON("v2"), pagingVersionJSON("v2")}
	for i, want := range wantVersions {
		if got := ext(t, reqs[i], "dataVersion"); got != want {
			t.Errorf("request %d dataVersion = %q, want %q", i+1, got, want)
		}
	}
	if got := ext(t, reqs[2], "topicSeq"); got != "0" {
		t.Errorf("restart request topicSeq = %q, want 0", got)
	}
	if got := ext(t, reqs[3], "topicSeq"); got != "2" {
		t.Errorf("request after restart topicSeq = %q, want 2", got)
	}
	for _, name := range []string{"A", "B", "C", "D"} {
		if wrapper.TopicConfigTable[name] == nil {
			t.Errorf("topic %s missing from the restarted table: %v", name, keysOfTopicTable(wrapper))
		}
	}
	if len(wrapper.TopicConfigTable) != 4 {
		t.Errorf("merged topics = %d, want 4", len(wrapper.TopicConfigTable))
	}
	if got := string(remoting.EncodeJSON(wrapper.DataVersion)); got != pagingVersionJSON("v2") {
		t.Errorf("merged dataVersion = %q, want %q", got, pagingVersionJSON("v2"))
	}
}

// An older broker returns the whole table in one answer and sends no
// totalTopicNum; the loop must stop instead of paging forever.
func TestAdminGetAllTopicConfigStopsWithoutTotalTopicNum(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetAllTopicConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		w := remoting.NewTopicConfigSerializeWrapper()
		w.TopicConfigTable["T1"] = remoting.NewTopicConfig("T1")
		w.TopicConfigTable["T2"] = remoting.NewTopicConfig("T2")
		w.DataVersion = pagingVersion("v1")
		return respOK(w.Encode())
	}

	wrapper, err := c.admin.GetAllTopicConfig(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllTopicConfig", err)
	if got := c.brokerLog.count(remoting.ReqGetAllTopicConfig); got != 1 {
		t.Fatalf("pages requested = %d, want exactly 1 without totalTopicNum", got)
	}
	if len(wrapper.TopicConfigTable) != 2 {
		t.Errorf("merged topics = %d, want 2", len(wrapper.TopicConfigTable))
	}
	if got := string(remoting.EncodeJSON(wrapper.DataVersion)); got != pagingVersionJSON("v1") {
		t.Errorf("merged dataVersion = %q, want %q", got, pagingVersionJSON("v1"))
	}
}

// The paging loop resolves the broker address once and keeps the VIP rewrite
// at its invoke boundary. A page request that forgot the rewrite would go to
// the published port and never arrive.
func TestAdminGetAllTopicConfigPagingKeepsVipRewrite(t *testing.T) {
	onePage := func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		w := remoting.NewTopicConfigSerializeWrapper()
		w.TopicConfigTable["VipTopic"] = remoting.NewTopicConfig("VipTopic")
		resp := respOK(w.Encode())
		resp.AddExtField("totalTopicNum", "1")
		return resp
	}

	on := newAdminCluster(t)
	on.admin.SetVipChannelEnabled(true)
	on.brokerAnswer[remoting.ReqGetAllTopicConfig] = onePage
	published := shiftPort(t, on.brokerSrv.addr, 2)
	wrapper, err := on.admin.GetAllTopicConfig(published, 0)
	requireNoError(t, "getAllTopicConfig over the VIP address", err)
	if wrapper.TopicConfigTable["VipTopic"] == nil {
		t.Fatalf("topics = %v", keysOfTopicTable(wrapper))
	}
	if got := on.brokerLog.count(remoting.ReqGetAllTopicConfig); got != 1 {
		t.Errorf("requests that arrived = %d, want 1 on the shifted port", got)
	}

	// Control: with the rewrite off the same published address is not
	// reachable, so the page cannot arrive at all.
	off := newAdminCluster(t)
	off.brokerAnswer[remoting.ReqGetAllTopicConfig] = onePage
	if _, err := off.admin.GetAllTopicConfig(shiftPort(t, off.brokerSrv.addr, 2), 0); err == nil {
		t.Fatal("the shifted address must fail while the VIP rewrite is off")
	}
	if got := off.brokerLog.count(remoting.ReqGetAllTopicConfig); got != 0 {
		t.Errorf("requests that arrived with the rewrite off = %d, want 0", got)
	}
}

// The admin timeout is a budget for the WHOLE walk: every page spends from the
// same clock. A loop that hands each page a fresh budget would let a slow
// broker stretch the call indefinitely.
func TestAdminGetAllTopicConfigPagingSharesAdminTimeout(t *testing.T) {
	slowPages := func(t *testing.T) func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		t.Helper()
		var mu sync.Mutex
		call := 0
		return func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
			mu.Lock()
			index := call
			call++
			mu.Unlock()
			if index == 0 {
				time.Sleep(300 * time.Millisecond)
			} else {
				time.Sleep(600 * time.Millisecond)
			}
			seq, _ := req.ExtFields().Get("topicSeq")
			names := []string{"T1", "T2"}
			if seq == "2" {
				names = []string{"T3", "T4"}
			}
			w := remoting.NewTopicConfigSerializeWrapper()
			for _, name := range names {
				w.TopicConfigTable[name] = remoting.NewTopicConfig(name)
			}
			w.DataVersion = pagingVersion("v1")
			resp := respOK(w.Encode())
			resp.AddExtField("totalTopicNum", "4")
			return resp
		}
	}

	short := newAdminCluster(t)
	short.admin.SetTimeoutMillis(800)
	short.brokerAnswer[remoting.ReqGetAllTopicConfig] = slowPages(t)
	_, err := short.admin.GetAllTopicConfig(short.brokerSrv.addr, 0)
	if err == nil {
		t.Fatal("the second page must spend the remaining budget and time out")
	}
	if !common.IsKind(err, common.KindTimeout) {
		t.Fatalf("error = %v, want a timeout from the shared budget", err)
	}
	if got := short.brokerLog.count(remoting.ReqGetAllTopicConfig); got != 2 {
		t.Errorf("pages attempted = %d, want 2", got)
	}

	// Control: the same delayed handler completes when the budget covers both
	// pages, so the failure above is the budget and not the handler.
	long := newAdminCluster(t)
	long.admin.SetTimeoutMillis(5000)
	long.brokerAnswer[remoting.ReqGetAllTopicConfig] = slowPages(t)
	wrapper, err := long.admin.GetAllTopicConfig(long.brokerSrv.addr, 0)
	requireNoError(t, "getAllTopicConfig with a large budget", err)
	if len(wrapper.TopicConfigTable) != 4 {
		t.Errorf("topics = %d, want 4", len(wrapper.TopicConfigTable))
	}
}

// ------------------------------------------------------- subscription groups

func TestAdminGetAllSubscriptionGroupPaginatesByReturnedCount(t *testing.T) {
	c := newAdminCluster(t)
	c.admin.SetMaxPageSizeInGetMetadata(5)

	pages := map[string][]string{
		"0": {"G1", "G2"},
		"2": {"G3", "G4"},
	}
	c.brokerAnswer[remoting.ReqGetAllSubscriptionGroupConfig] = func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		seq, _ := req.ExtFields().Get("groupSeq")
		w := remoting.NewSubscriptionGroupWrapper()
		for _, name := range pages[seq] {
			w.SubscriptionGroupTable[name] = remoting.NewSubscriptionGroupConfig(name)
		}
		w.DataVersion = pagingVersion("v1")
		resp := respOK(w.Encode())
		resp.AddExtField("totalGroupNum", "4")
		return resp
	}

	wrapper, err := c.admin.GetAllSubscriptionGroup(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllSubscriptionGroup", err)

	reqs := c.brokerLog.all(remoting.ReqGetAllSubscriptionGroupConfig)
	if len(reqs) != 2 {
		t.Fatalf("pages requested = %d, want 2", len(reqs))
	}
	for k, want := range map[string]string{"groupSeq": "0", "maxGroupNum": "5"} {
		if got := ext(t, reqs[0], k); got != want {
			t.Errorf("first page %s = %q, want %q", k, got, want)
		}
	}
	if got := ext(t, reqs[0], "dataVersion"); got != "" {
		t.Errorf("first page dataVersion = %q, want the present-but-empty string", got)
	}
	if got := ext(t, reqs[1], "groupSeq"); got != "2" {
		t.Errorf("second page groupSeq = %q, want 2 (returned entries, not maxGroupNum)", got)
	}
	if got := ext(t, reqs[1], "maxGroupNum"); got != "5" {
		t.Errorf("second page maxGroupNum = %q, want 5", got)
	}
	if got, want := ext(t, reqs[1], "dataVersion"), pagingVersionJSON("v1"); got != want {
		t.Errorf("second page dataVersion = %q, want %q", got, want)
	}
	for _, name := range []string{"G1", "G2", "G3", "G4"} {
		if wrapper.SubscriptionGroupTable[name] == nil {
			t.Errorf("group %s missing from the merged table: %v", name, keysOfGroupTable(wrapper))
		}
	}
	if len(wrapper.SubscriptionGroupTable) != 4 {
		t.Errorf("merged groups = %d, want 4", len(wrapper.SubscriptionGroupTable))
	}
	if got := string(remoting.EncodeJSON(wrapper.DataVersion)); got != pagingVersionJSON("v1") {
		t.Errorf("merged dataVersion = %q, want %q", got, pagingVersionJSON("v1"))
	}
}

func TestAdminGetAllSubscriptionGroupRestartsOnVersionChange(t *testing.T) {
	c := newAdminCluster(t)

	pages := []struct {
		version string
		names   []string
	}{
		{"v1", []string{"G1", "G2"}},
		{"v2", []string{"G3"}},
		{"v2", []string{"G1", "G2"}},
		{"v2", []string{"G3", "G4"}},
	}
	var mu sync.Mutex
	next := 0
	c.brokerAnswer[remoting.ReqGetAllSubscriptionGroupConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		mu.Lock()
		page := pages[next]
		next++
		mu.Unlock()
		w := remoting.NewSubscriptionGroupWrapper()
		for _, name := range page.names {
			w.SubscriptionGroupTable[name] = remoting.NewSubscriptionGroupConfig(name)
		}
		w.DataVersion = pagingVersion(page.version)
		resp := respOK(w.Encode())
		resp.AddExtField("totalGroupNum", "4")
		return resp
	}

	wrapper, err := c.admin.GetAllSubscriptionGroup(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllSubscriptionGroup", err)

	reqs := c.brokerLog.all(remoting.ReqGetAllSubscriptionGroupConfig)
	if len(reqs) != 4 {
		t.Fatalf("pages requested = %d, want 4", len(reqs))
	}
	wantVersions := []string{"", pagingVersionJSON("v1"), pagingVersionJSON("v2"), pagingVersionJSON("v2")}
	for i, want := range wantVersions {
		if got := ext(t, reqs[i], "dataVersion"); got != want {
			t.Errorf("request %d dataVersion = %q, want %q", i+1, got, want)
		}
	}
	if got := ext(t, reqs[2], "groupSeq"); got != "0" {
		t.Errorf("restart request groupSeq = %q, want 0", got)
	}
	if got := ext(t, reqs[3], "groupSeq"); got != "2" {
		t.Errorf("request after restart groupSeq = %q, want 2", got)
	}
	for _, name := range []string{"G1", "G2", "G3", "G4"} {
		if wrapper.SubscriptionGroupTable[name] == nil {
			t.Errorf("group %s missing from the restarted table: %v", name, keysOfGroupTable(wrapper))
		}
	}
	if len(wrapper.SubscriptionGroupTable) != 4 {
		t.Errorf("merged groups = %d, want 4", len(wrapper.SubscriptionGroupTable))
	}
	if got := string(remoting.EncodeJSON(wrapper.DataVersion)); got != pagingVersionJSON("v2") {
		t.Errorf("merged dataVersion = %q, want %q", got, pagingVersionJSON("v2"))
	}
}

// The restart must also clear the forbidden table accumulated from the
// discarded pages; a stale entry would otherwise describe a group the new
// snapshot never returned.
func TestAdminGetAllSubscriptionGroupRestartClearsForbiddenTable(t *testing.T) {
	c := newAdminCluster(t)

	var mu sync.Mutex
	next := 0
	c.brokerAnswer[remoting.ReqGetAllSubscriptionGroupConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		mu.Lock()
		index := next
		next++
		mu.Unlock()
		w := remoting.NewSubscriptionGroupWrapper()
		switch index {
		case 0:
			w.SubscriptionGroupTable["G1"] = remoting.NewSubscriptionGroupConfig("G1")
			w.SubscriptionGroupTable["G2"] = remoting.NewSubscriptionGroupConfig("G2")
			w.ForbiddenTable["G1"] = "forbidden-before"
			w.DataVersion = pagingVersion("v1")
		case 1:
			w.SubscriptionGroupTable["G3"] = remoting.NewSubscriptionGroupConfig("G3")
			w.DataVersion = pagingVersion("v2")
		case 2:
			w.SubscriptionGroupTable["G1"] = remoting.NewSubscriptionGroupConfig("G1")
			w.SubscriptionGroupTable["G2"] = remoting.NewSubscriptionGroupConfig("G2")
			w.DataVersion = pagingVersion("v2")
		default:
			w.SubscriptionGroupTable["G3"] = remoting.NewSubscriptionGroupConfig("G3")
			w.SubscriptionGroupTable["G4"] = remoting.NewSubscriptionGroupConfig("G4")
			w.DataVersion = pagingVersion("v2")
		}
		resp := respOK(w.Encode())
		resp.AddExtField("totalGroupNum", "4")
		return resp
	}

	wrapper, err := c.admin.GetAllSubscriptionGroup(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllSubscriptionGroup", err)
	if len(wrapper.SubscriptionGroupTable) != 4 {
		t.Fatalf("groups = %v, want G1..G4", keysOfGroupTable(wrapper))
	}
	if len(wrapper.ForbiddenTable) != 0 {
		t.Errorf("forbiddenTable = %v, want it cleared with the restarted table", wrapper.ForbiddenTable)
	}
}
