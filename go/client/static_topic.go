// Static topic (unitized topic) routing support, ported from the Java client:
//   - org.apache.rocketmq.remoting.rpc.ClientMetadata
//     .topicRouteData2EndpointsForStaticTopic
//   - org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils
//     .getMockBrokerName
//   - org.apache.rocketmq.client.impl.factory.MQClientInstance
//     .getBrokerNameFromMessageQueue (see Instance.GetBrokerNameFromMessageQueue)
//
// Wire facts (do NOT "simplify"):
//   - The route decodes topicQueueMappingByBroker as an opaque
//     []MappingEntry (broker -> Info json object). The typed view is parsed
//     HERE, not in the protocol layer, so the pass-through stays untouched.
//   - TopicQueueMappingInfo.scope defaults to MixAll.METADATA_SCOPE_GLOBAL
//     ("__global__"); the mock broker name for the global scope strips the
//     leading "__" ("__syslo__global__").
//   - Java's maxTotalNumOfEpoch is read but never reassigned (stays -1), so
//     the `epoch >= maxTotalNumOfEpoch` guard is always true and maxTotalNums
//     is simply the max totalQueues of the scope — quirk kept.
//   - A later equal-epoch entry OVERWRITES an earlier one (Java
//     `oldEpoch <= epoch`).
package client

import (
	"sort"
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// MixAll.METADATA_SCOPE_GLOBAL / LOGICAL_QUEUE_MOCK_BROKER_PREFIX /
// LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST.
const (
	metadataScopeGlobal          = "__global__"
	logicalQueueMockBrokerPrefix = "__syslo__"
	logicalQueueMockNotExist     = "__syslo__none__"
)

func isLogicalQueueMockBrokerName(brokerName string) bool {
	return strings.HasPrefix(brokerName, logicalQueueMockBrokerPrefix)
}

// getMockBrokerName: "__global__" (the scope) loses its first two chars,
// every other scope is prefixed verbatim.
func getMockBrokerName(scope string) string {
	if scope == metadataScopeGlobal {
		return logicalQueueMockBrokerPrefix + scope[2:]
	}
	return logicalQueueMockBrokerPrefix + scope
}

// topicQueueMappingInfo is the typed view of one
// topicQueueMappingByBroker[brokerName] entry (Java
// TopicQueueMappingInfo).
type topicQueueMappingInfo struct {
	scope       string
	totalQueues int
	bname       string
	epoch       int64
	currIDMap   map[int]int
}

// parseTopicQueueMappingInfo reads the typed view out of the raw json object
// the route decoder kept opaque. Malformed entries are dropped (ok=false) —
// Java would NPE on them; a salvageable partial mapping beats a crash.
func parseTopicQueueMappingInfo(raw any) (topicQueueMappingInfo, bool) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return topicQueueMappingInfo{}, false
	}
	info := topicQueueMappingInfo{scope: metadataScopeGlobal}
	if s, ok := obj["scope"].(string); ok {
		info.scope = s
	}
	if n, ok := obj["totalQueues"].(float64); ok {
		info.totalQueues = int(n)
	}
	if s, ok := obj["bname"].(string); ok {
		info.bname = s
	}
	if n, ok := obj["epoch"].(float64); ok {
		info.epoch = int64(n)
	}
	if m, ok := obj["currIdMap"].(map[string]any); ok {
		info.currIDMap = make(map[int]int, len(m))
		for k, v := range m {
			gid, err := strconv.Atoi(k)
			if err != nil {
				continue
			}
			phys, ok := v.(float64)
			if !ok {
				continue
			}
			info.currIDMap[gid] = int(phys)
		}
	}
	return info, true
}

// topicRouteData2EndpointsForStaticTopic builds the LOGICAL queue set:
// MessageQueue(topic, mockBrokerName(scope), globalId) -> the REAL broker name
// hosting it ("__syslo__none__" when a logical id inside the totals has no
// hosting broker). A nil/empty mapping (non-static topic) yields nil.
func topicRouteData2EndpointsForStaticTopic(topic string, route *TopicRouteData) map[common.MessageQueue]string {
	if route == nil || len(route.TopicQueueMappingByBroker) == 0 {
		return nil
	}

	// group the infos by scope (entries WITHOUT a scope do not participate,
	// as in Java).
	infosByScope := map[string]map[string]topicQueueMappingInfo{}
	for _, entry := range route.TopicQueueMappingByBroker {
		info, ok := parseTopicQueueMappingInfo(entry.Info)
		if !ok {
			continue
		}
		if infosByScope[info.scope] == nil {
			infosByScope[info.scope] = map[string]topicQueueMappingInfo{}
		}
		infosByScope[info.scope][entry.Broker] = info
	}

	endpoints := map[common.MessageQueue]string{}
	for scope, infos := range infosByScope {
		// epoch-desc with a deterministic (broker asc) tie-break. Java sorts
		// epoch-desc over a ConcurrentHashMap's entry set, so its equal-epoch
		// overwrite order is unspecified there anyway; pinning broker-asc
		// makes the result reproducible.
		names := make([]string, 0, len(infos))
		for name := range infos {
			names = append(names, name)
		}
		sort.Strings(names)
		type nameInfo struct {
			name string
			info topicQueueMappingInfo
		}
		ordered := make([]nameInfo, 0, len(names))
		for _, name := range names {
			ordered = append(ordered, nameInfo{name, infos[name]})
		}
		sort.SliceStable(ordered, func(i, j int) bool {
			return ordered[i].info.epoch > ordered[j].info.epoch
		})

		maxTotalNums := 0
		maxTotalNumOfEpoch := int64(-1) // Java never updates this — guard always true
		mqEndPoints := map[common.MessageQueue]topicQueueMappingInfo{}
		for _, ni := range ordered {
			if ni.info.epoch >= maxTotalNumOfEpoch && ni.info.totalQueues > maxTotalNums {
				maxTotalNums = ni.info.totalQueues
			}
			// Only the global ids (keys) shape the endpoint set; the physical
			// ids are broker-internal.
			for globalID := range ni.info.currIDMap {
				mq := common.NewMessageQueue(topic, getMockBrokerName(scope), int32(globalID))
				old, ok := mqEndPoints[mq]
				if !ok || old.epoch <= ni.info.epoch {
					mqEndPoints[mq] = ni.info
				}
			}
		}

		// Accomplish the static logic queues: every id below the total
		// exists; unhosted ones point at the NOT_EXIST sentinel.
		for i := 0; i < maxTotalNums; i++ {
			mq := common.NewMessageQueue(topic, getMockBrokerName(scope), int32(i))
			if holder, ok := mqEndPoints[mq]; ok {
				endpoints[mq] = holder.bname
			} else {
				endpoints[mq] = logicalQueueMockNotExist
			}
		}
	}
	if len(endpoints) == 0 {
		return nil
	}
	return endpoints
}
