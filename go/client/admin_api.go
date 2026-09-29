// Instance-level management RPCs (the Java MQClientInstance / MQAdminImpl
// helpers the DefaultMQAdminExt surface sits on): nameserver queries, topic
// creation/deletion and the java.util.Properties broker-config codec.
//
// These live on Instance rather than on DefaultMQAdminExt because Java puts
// them there too — `createTopicInRoute` walks the DEFAULT_TOPIC route and
// `getBrokerClusterInfo` loops the nameserver list, both of which need the
// instance's address book.
package client

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------- extFields helpers ----------------

// adminExt builds an extFields map from an alternating key/value list. Values
// are rendered the way Java's `RemotingCommand.makeCustomHeaderToNet` does it
// (String.valueOf for ints, "true"/"false" for bools), and — importantly —
// the keys written here are the Java FIELD names, because the broker reads
// them back by reflection. That is exactly where the `isForce`-not-`force` and
// `cluster`-not-`clusterName` traps live, so keep the names inline and visible.
func adminExt(kv ...any) *common.StringMap {
	out := common.NewStringMap()
	for i := 0; i+1 < len(kv); i += 2 {
		key, _ := kv[i].(string)
		out.Put(key, adminExtValue(kv[i+1]))
	}
	return out
}

func adminExtValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", t)
	case int32:
		return fmt.Sprintf("%d", t)
	case int64:
		return fmt.Sprintf("%d", t)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

// adminRequest builds a request whose extFields are set directly (the Python
// reference passes plain dicts; this port does the same so the Java field
// names stay visible at the call site).
func adminRequest(code int32, ext *common.StringMap) *remoting.RemotingCommand {
	cmd := remoting.CreateRequestCommand(code, nil)
	if ext != nil {
		ext.Range(func(k, v string) { cmd.AddExtField(k, v) })
	}
	return cmd
}

// ---------------- nameserver queries ----------------

// GetBrokerClusterInfo sends GET_BROKER_CLUSTER_INFO(106) to the nameservers,
// returning the first successful answer.
func (i *Instance) GetBrokerClusterInfo(timeoutMillis int64) (*remoting.ClusterInfo, error) {
	request := adminRequest(remoting.ReqGetBrokerClusterInfo, nil)
	for _, nsAddr := range i.NameServerAddrs() {
		response, err := i.invokeSync(nsAddr, request, timeoutMillis)
		if err != nil {
			common.LogDebugf("getBrokerClusterInfo from %s failed: %v", nsAddr, err)
			continue
		}
		if response.Code == remoting.RespSuccess && len(response.Body) > 0 {
			return remoting.DecodeClusterInfo(response.Body)
		}
	}
	return nil, common.ClientError("Failed to get broker cluster info from name server")
}

// GetAllTopicListFromNameServer sends GET_ALL_TOPIC_LIST_FROM_NAMESERVER(206).
func (i *Instance) GetAllTopicListFromNameServer(timeoutMillis int64) (*remoting.TopicList, error) {
	request := adminRequest(remoting.ReqGetAllTopicListFromNameServer, nil)
	for _, nsAddr := range i.NameServerAddrs() {
		response, err := i.invokeSync(nsAddr, request, timeoutMillis)
		if err != nil {
			common.LogDebugf("getAllTopicListFromNameServer from %s failed: %v", nsAddr, err)
			continue
		}
		if response.Code == remoting.RespSuccess && len(response.Body) > 0 {
			return remoting.DecodeTopicList(response.Body)
		}
	}
	return nil, common.ClientError("Failed to get all topic list from name server")
}

// GetSystemTopicListFromBroker sends GET_SYSTEM_TOPIC_LIST_FROM_BROKER(305).
func (i *Instance) GetSystemTopicListFromBroker(addr string, timeoutMillis int64) (*remoting.TopicList, error) {
	request := adminRequest(remoting.ReqGetSystemTopicListFromBroker, nil)
	response, err := i.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if err := i.checkResponse(response); err != nil {
		return nil, err
	}
	return remoting.DecodeTopicList(response.Body)
}

// ---------------- topic creation ----------------

// AdminCreateTopicRetryTimes mirrors MQAdminImpl.createTopic's per-broker
// retry count.
const AdminCreateTopicRetryTimes = 5

// CreateTopicInBroker sends UPDATE_AND_CREATE_TOPIC(17) to one broker.
//
// `topicFilterType` MUST be sent: the broker's
// CreateTopicRequestHeader.checkFields() parses it into an enum and rejects a
// null with `topicFilterType = [null] value invalid`, so even a plain topic
// needs the field.
//
// Broker-business errors (a non-SUCCESS *response*) are raised immediately;
// only transport errors are retried, matching Java.
func (i *Instance) CreateTopicInBroker(addr, defaultTopic, topic string,
	readQueueNums, writeQueueNums, perm, topicSysFlag int32,
	topicFilterType string, order bool, attributes string,
	timeoutMillis int64, retryTimes int) error {

	if topicFilterType == "" {
		topicFilterType = remoting.TopicFilterTypeSingleTag
	}
	if retryTimes < 1 {
		retryTimes = 1
	}
	var lastErr error
	for attempt := 0; attempt < retryTimes; attempt++ {
		request := adminRequest(remoting.ReqUpdateAndCreateTopic, adminExt(
			"topic", topic,
			"defaultTopic", defaultTopic,
			"readQueueNums", readQueueNums,
			"writeQueueNums", writeQueueNums,
			"perm", perm,
			"topicFilterType", topicFilterType,
			"topicSysFlag", topicSysFlag,
			"order", order,
			// Java: AttributeParser.parseToString(map) — an empty map renders
			// as "", not null.
			"attributes", attributes,
			"force", false,
		))
		response, err := i.invokeSync(addr, request, timeoutMillis)
		if err != nil {
			lastErr = err
			continue
		}
		if err := i.checkResponse(response); err != nil {
			// A broker rejection is final — Java rethrows MQBrokerException
			// straight out of the retry loop.
			return err
		}
		return nil
	}
	return lastErr
}

// CreateTopicInRoute mirrors Java MQAdminImpl.createTopic: push the topic to
// every MASTER in the DEFAULT_TOPIC (TBW102) route. Succeeding on at least one
// broker is enough; if every broker fails, the last error is reported.
func (i *Instance) CreateTopicInRoute(topic string, readQueueNums, writeQueueNums, perm, topicSysFlag int32,
	attributes string, timeoutMillis int64) error {

	route := i.GetTopicRouteData(common.DefaultTopic)
	if route == nil {
		return common.ClientError(fmt.Sprintf("No route info of default topic %s", common.DefaultTopic))
	}
	created := false
	var lastErr error
	for _, bd := range route.BrokerDatas {
		addr, ok := bd.SelectBrokerAddr()
		if !ok {
			continue
		}
		err := i.CreateTopicInBroker(addr, common.DefaultTopic, topic,
			readQueueNums, writeQueueNums, perm, topicSysFlag,
			remoting.TopicFilterTypeSingleTag, false, attributes,
			timeoutMillis, AdminCreateTopicRetryTimes)
		if err != nil {
			lastErr = err
			continue
		}
		created = true
	}
	if !created && lastErr != nil {
		return common.ClientError(fmt.Sprintf("create new topic failed: %v", lastErr))
	}
	return nil
}

// ---------------- topic deletion ----------------

// DeleteTopicInBroker sends DELETE_TOPIC_IN_BROKER(215) to one broker.
func (i *Instance) DeleteTopicInBroker(addr, topic string, timeoutMillis int64) error {
	response, err := i.invokeSync(addr, adminRequest(remoting.ReqDeleteTopicInBroker, adminExt("topic", topic)), timeoutMillis)
	if err != nil {
		return err
	}
	return i.checkResponse(response)
}

// DeleteTopicInNameSrv sends DELETE_TOPIC_IN_NAMESRV(216), first nameserver
// that answers wins.
func (i *Instance) DeleteTopicInNameSrv(topic string, timeoutMillis int64) error {
	request := adminRequest(remoting.ReqDeleteTopicInNameSrv, adminExt("topic", topic))
	var lastErr error
	for _, nsAddr := range i.NameServerAddrs() {
		response, err := i.invokeSync(nsAddr, request, timeoutMillis)
		if err != nil {
			lastErr = err
			continue
		}
		if err := i.checkResponse(response); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr != nil {
		return common.ClientError(fmt.Sprintf("Failed to delete topic %s in name server: %v", topic, lastErr))
	}
	return common.ClientError(fmt.Sprintf("Failed to delete topic %s in name server", topic))
}

// ---------------- java.util.Properties codec ----------------

// String2Properties mirrors Java MixAll.string2Properties, i.e. the semantics
// of `java.util.Properties.load`:
//
//   - blank lines and `#` / `!` comment lines are skipped;
//   - an UNESCAPED trailing `\` continues onto the next line, whose leading
//     whitespace is dropped;
//   - the key ends at the first `=`, `:` or WHITESPACE (whitespace is a legal
//     separator too);
//   - whitespace around the separator is skipped; the value's TRAILING
//     whitespace is preserved (Java does not trim it).
//
// This matters because GET_BROKER_CONFIG(26) returns properties TEXT, not
// JSON — a client that parses it as a KVTable fails on every real broker.
//
// Java also expands `\t`, `\n`, `\uXXXX`; broker config exports never contain
// them and getting the backslash semantics wrong would be worse than omitting
// it, so it is deliberately not implemented here (the Python port makes the
// same call).
func String2Properties(text string) map[string]string {
	result := map[string]string{}
	if text == "" {
		return result
	}
	const propWS = " \t\f"

	// First merge continuation lines into logical lines.
	var logical []string
	pending := ""
	hasPending := false
	for _, raw := range splitLines(text) {
		line := raw
		if hasPending {
			line = pending + strings.TrimLeft(line, propWS)
			hasPending = false
		}
		trailing := 0
		for k := len(line) - 1; k >= 0 && line[k] == '\\'; k-- {
			trailing++
		}
		if trailing%2 == 1 {
			pending = line[:len(line)-1]
			hasPending = true
			continue
		}
		logical = append(logical, line)
	}
	if hasPending {
		logical = append(logical, pending)
	}

	for _, line := range logical {
		stripped := strings.TrimSpace(line)
		if stripped == "" || stripped[0] == '#' || stripped[0] == '!' {
			continue
		}
		n := len(line)
		pos := 0
		for pos < n && strings.IndexByte(propWS, line[pos]) >= 0 {
			pos++
		}
		keyStart := pos
		for pos < n && line[pos] != '=' && line[pos] != ':' && strings.IndexByte(propWS, line[pos]) < 0 {
			pos++
		}
		key := line[keyStart:pos]
		// Skip whitespace before the separator.
		for pos < n && strings.IndexByte(propWS, line[pos]) >= 0 {
			pos++
		}
		// Optional '=' / ':' plus the whitespace after it.
		if pos < n && (line[pos] == '=' || line[pos] == ':') {
			pos++
			for pos < n && strings.IndexByte(propWS, line[pos]) >= 0 {
				pos++
			}
		}
		result[key] = line[pos:]
	}
	return result
}

// splitLines mirrors Java's BufferedReader line splitting closely enough for
// properties text: it splits on \n and drops a trailing \r, and never yields
// the empty tail after a final newline.
func splitLines(text string) []string {
	if text == "" {
		return nil
	}
	raw := strings.Split(text, "\n")
	if len(raw) > 0 && raw[len(raw)-1] == "" {
		raw = raw[:len(raw)-1]
	}
	for idx, line := range raw {
		raw[idx] = strings.TrimSuffix(line, "\r")
	}
	return raw
}

// ---------------- misc instance helpers used by the admin ----------------

// FirstBrokerAddr is Java DefaultMQAdminExtImpl's "pick any broker" fallback:
// the first address of the cluster info, else an error.
func (i *Instance) FirstBrokerAddr() (string, error) {
	cluster, err := i.GetBrokerClusterInfo(10_000)
	if err == nil {
		if addrs := cluster.BrokerAddrs(); len(addrs) > 0 {
			return addrs[0], nil
		}
	}
	return "", common.ClientError("no broker address available")
}

// BrokerAddrsOfCluster returns every broker address of one cluster, or of the
// whole cluster info when clusterName is empty.
func (i *Instance) BrokerAddrsOfCluster(clusterName string) ([]string, error) {
	cluster, err := i.GetBrokerClusterInfo(10_000)
	if err != nil {
		return nil, err
	}
	if clusterName == "" {
		return cluster.BrokerAddrs(), nil
	}
	names := cluster.ClusterAddrTable[clusterName]
	if len(names) == 0 {
		return cluster.BrokerAddrs(), nil
	}
	seen := map[string]bool{}
	var out []string
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for _, brokerName := range sorted {
		entry := cluster.BrokerAddrTable[brokerName]
		if entry == nil {
			continue
		}
		for _, addr := range sortedAddrs(entry.BrokerAddrs) {
			if addr != "" && !seen[addr] {
				seen[addr] = true
				out = append(out, addr)
			}
		}
	}
	if len(out) == 0 {
		return cluster.BrokerAddrs(), nil
	}
	return out, nil
}

// sortedAddrs lists a broker's addresses ordered by brokerId.
func sortedAddrs(m map[int64]string) []string {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, m[id])
	}
	return out
}
