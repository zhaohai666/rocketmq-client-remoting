// Static topic routing tests: the typed mapping parse, the pure
// topicRouteData2EndpointsForStaticTopic port, and the Instance's
// GetBrokerNameFromMessageQueue rewrite.
package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func rawMapping(scope string, totalQueues int, bname string, epoch int64, currIDMap map[string]any) map[string]any {
	return map[string]any{
		"scope":       scope,
		"totalQueues": float64(totalQueues),
		"bname":       bname,
		"epoch":       float64(epoch),
		"dirty":       false,
		"currIdMap":   currIDMap,
	}
}

func TestGetMockBrokerNameStripsGlobalUnderscores(t *testing.T) {
	if got := getMockBrokerName("__global__"); got != "__syslo__global__" {
		t.Errorf("getMockBrokerName(__global__) = %q, want __syslo__global__", got)
	}
	if got := getMockBrokerName("zone-a"); got != "__syslo__zone-a" {
		t.Errorf("getMockBrokerName(zone-a) = %q, want __syslo__zone-a", got)
	}
	if !isLogicalQueueMockBrokerName("__syslo__global__") || isLogicalQueueMockBrokerName("broker-a") {
		t.Error("isLogicalQueueMockBrokerName misclassifies")
	}
}

func TestParseTopicQueueMappingInfoDefaults(t *testing.T) {
	info, ok := parseTopicQueueMappingInfo(map[string]any{
		"totalQueues": float64(2),
		"bname":       "b1",
		"epoch":       float64(3),
	})
	if !ok {
		t.Fatal("parse failed")
	}
	if info.scope != "__global__" {
		t.Errorf("scope = %q, want the __global__ default", info.scope)
	}
	if info.totalQueues != 2 || info.bname != "b1" || info.epoch != 3 {
		t.Errorf("info = %+v", info)
	}
	if _, ok := parseTopicQueueMappingInfo("not-an-object"); ok {
		t.Error("non-object raw must be rejected")
	}
}

func mappingRoute(entries ...MappingEntry) *TopicRouteData {
	return &TopicRouteData{TopicQueueMappingByBroker: entries}
}

func TestEndpointsSingleScopeWithUnhostedQueue(t *testing.T) {
	route := mappingRoute(MappingEntry{Broker: "broker-a", Info: rawMapping(
		"__global__", 4, "broker-a", 0,
		map[string]any{"0": float64(0), "1": float64(1), "2": float64(2)},
	)})
	endpoints := topicRouteData2EndpointsForStaticTopic("StaticTopic", route)
	if len(endpoints) != 4 {
		t.Fatalf("endpoints = %d entries, want 4 (hosted + unhosted)", len(endpoints))
	}
	for id := int32(0); id < 3; id++ {
		mq := common.NewMessageQueue("StaticTopic", "__syslo__global__", id)
		if got := endpoints[mq]; got != "broker-a" {
			t.Errorf("global %d -> %q, want broker-a", id, got)
		}
	}
	if got := endpoints[common.NewMessageQueue("StaticTopic", "__syslo__global__", 3)]; got != "__syslo__none__" {
		t.Errorf("unhosted global 3 -> %q, want the NOT_EXIST sentinel", got)
	}
	// nil/empty mapping (non-static topic) yields nil.
	if topicRouteData2EndpointsForStaticTopic("T", &TopicRouteData{}) != nil {
		t.Error("empty mapping must yield nil endpoints")
	}
}

// Regression: on the wire ParseJSON decodes numbers as remoting.JSONNumber,
// NOT float64 — a hand-built float64 fixture never caught that the typed
// mapping parse silently dropped every field and produced an empty endpoint
// set against a real nameserver (found on the live static-topic leg).
func TestEndpointsFromDecodedWireRoute(t *testing.T) {
	wire := `{
	  "orderTopicConf": null,
	  "queueDatas": [{"brokerName":"broker-a","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0}],
	  "brokerDatas": [{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":"127.0.0.1:10911"}}],
	  "topicQueueMappingByBroker": {
	    "broker-a": {
	      "topic": "StaticWire",
	      "scope": "__global__",
	      "totalQueues": 8,
	      "bname": "broker-a",
	      "epoch": 0,
	      "dirty": false,
	      "currIdMap": {"0":0,"1":1,"2":2,"3":3},
	      "hostedQueues": {
	        "0": [{"gen":0,"queueId":0,"bname":"broker-a","logicOffset":0,"startOffset":0,"endOffset":-1,"timeOfStart":-1,"timeOfEnd":-1}]
	      }
	    }
	  }
	}`
	route, err := DecodeTopicRouteData([]byte(wire))
	if err != nil {
		t.Fatalf("decode wire route: %v", err)
	}
	if len(route.TopicQueueMappingByBroker) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(route.TopicQueueMappingByBroker))
	}
	endpoints := topicRouteData2EndpointsForStaticTopic("StaticWire", route)
	if len(endpoints) != 8 {
		t.Fatalf("endpoints = %d entries, want 8 (JSONNumber fields were dropped?)", len(endpoints))
	}
	for id := int32(0); id < 4; id++ {
		if got := endpoints[common.NewMessageQueue("StaticWire", "__syslo__global__", id)]; got != "broker-a" {
			t.Errorf("hosted global %d -> %q, want broker-a", id, got)
		}
	}
	for id := int32(4); id < 8; id++ {
		if got := endpoints[common.NewMessageQueue("StaticWire", "__syslo__global__", id)]; got != "__syslo__none__" {
			t.Errorf("unhosted global %d -> %q, want the NOT_EXIST sentinel", id, got)
		}
	}
}

func TestEndpointsScopesStayApartAndEpochFences(t *testing.T) {
	route := mappingRoute(
		MappingEntry{Broker: "broker-old", Info: rawMapping("__global__", 1, "broker-old", 3, map[string]any{"0": float64(0)})},
		MappingEntry{Broker: "broker-new", Info: rawMapping("__global__", 1, "broker-new", 7, map[string]any{"0": float64(0)})},
		MappingEntry{Broker: "broker-b", Info: rawMapping("zone-x", 1, "broker-b", 9, map[string]any{"0": float64(0)})},
	)
	endpoints := topicRouteData2EndpointsForStaticTopic("T", route)
	if got := endpoints[common.NewMessageQueue("T", "__syslo__global__", 0)]; got != "broker-new" {
		t.Errorf("global 0 -> %q, want broker-new (higher epoch fences)", got)
	}
	if got := endpoints[common.NewMessageQueue("T", "__syslo__zone-x", 0)]; got != "broker-b" {
		t.Errorf("zone-x 0 -> %q, want broker-b", got)
	}
	if len(endpoints) != 2 {
		t.Errorf("endpoints = %d, want 2 (scopes do not merge)", len(endpoints))
	}
}

func TestInstanceGetBrokerNameFromMessageQueue(t *testing.T) {
	inst := newInstance("static-test@DEFAULT", []string{"127.0.0.1:9876"}, ClientInstanceConfig{})
	endpoints := map[common.MessageQueue]string{
		common.NewMessageQueue("StaticTopic", "__syslo__global__", 0): "broker-a",
		common.NewMessageQueue("StaticTopic", "__syslo__global__", 1): "broker-b",
	}
	inst.mu.Lock()
	inst.topicEndPointsTable["StaticTopic"] = endpoints
	inst.mu.Unlock()

	if got := inst.GetBrokerNameFromMessageQueue(common.NewMessageQueue("StaticTopic", "__syslo__global__", 0)); got != "broker-a" {
		t.Errorf("global 0 -> %q, want broker-a", got)
	}
	if got := inst.GetBrokerNameFromMessageQueue(common.NewMessageQueue("StaticTopic", "__syslo__global__", 1)); got != "broker-b" {
		t.Errorf("global 1 -> %q, want broker-b", got)
	}
	// Unknown queue / non-static topic pass their own broker name through.
	if got := inst.GetBrokerNameFromMessageQueue(common.NewMessageQueue("StaticTopic", "__syslo__global__", 99)); got != "__syslo__global__" {
		t.Errorf("unknown global 99 -> %q, want the mock name verbatim", got)
	}
	if got := inst.GetBrokerNameFromMessageQueue(common.NewMessageQueue("Plain", "broker-a", 0)); got != "broker-a" {
		t.Errorf("non-static -> %q, want broker-a", got)
	}
}
