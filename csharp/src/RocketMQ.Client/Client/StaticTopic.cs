using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

// The test project has no InternalsVisibleTo relationship with this assembly, so these are public; do not use for business code
// (the business entry points are MqClient.GetBrokerNameFromMessageQueue / PopAckAddrFor).
public static class StaticTopic
{
    // MixAll.METADATA_SCOPE_GLOBAL / LOGICAL_QUEUE_MOCK_BROKER_PREFIX /
    // LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST.
    public const string MetadataScopeGlobal = "__global__";
    public const string LogicalQueueMockBrokerPrefix = "__syslo__";
    public const string LogicalQueueMockNotExist = "__syslo__none__";

    public static bool IsLogicalQueueMockBrokerName(string brokerName) =>
        brokerName.StartsWith(LogicalQueueMockBrokerPrefix, StringComparison.Ordinal);

    /// <summary>
    /// getMockBrokerName: "__global__" (the scope itself) strips its first two characters; other scopes keep their value with a prefix added.
    /// </summary>
    public static string GetMockBrokerName(string scope)
    {
        if (scope == MetadataScopeGlobal)
        {
            return LogicalQueueMockBrokerPrefix + scope[2..];
        }

        return LogicalQueueMockBrokerPrefix + scope;
    }

    public sealed class TopicQueueMappingInfo
    {
        public string Scope = MetadataScopeGlobal;
        public int TotalQueues;
        public string Bname = string.Empty;
        public long Epoch;
        // globalId -> physical queueId. The physical id is not used in endpoint derivation, kept only for troubleshooting.
        public readonly SortedDictionary<int, int> CurrIdMap = new();
    }

    public static bool ParseTopicQueueMappingInfo(JsonValue raw,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TopicQueueMappingInfo? info)
    {
        info = null;
        if (!raw.IsObject)
        {
            return false;
        }

        info = new TopicQueueMappingInfo();
        if (raw.Find("scope") is { IsString: true } scope)
        {
            info.Scope = scope.StringValue();
        }

        if (raw.Find("totalQueues") is { IsNumber: true } totalQueues)
        {
            info.TotalQueues = (int)totalQueues.IntValue();
        }

        if (raw.Find("bname") is { IsString: true } bname)
        {
            info.Bname = bname.StringValue();
        }

        if (raw.Find("epoch") is { IsNumber: true } epoch)
        {
            info.Epoch = epoch.IntValue();
        }

        if (raw.Find("currIdMap") is { IsObject: true } currIdMap)
        {
            foreach (var kv in currIdMap.ObjectItems())
            {
                // The key is the decimal string of globalId; entries that fail to parse are skipped.
                if (!int.TryParse(kv.Key, NumberStyles.Integer, CultureInfo.InvariantCulture,
                        out int gid) || !kv.Value.IsNumber)
                {
                    continue;
                }

                info.CurrIdMap[gid] = (int)kv.Value.IntValue();
            }
        }

        return true;
    }

    /// <summary>
    /// ClientMetadata.topicRouteData2EndpointsForStaticTopic: build the logical queue set
    /// MessageQueue(topic, mockBrokerName(scope), globalId) -> the real broker name that hosts it
    /// (logical ids with no host within the total range point to "__syslo__none__").
    /// An empty mapping (non-static topic) returns false and leaves out unchanged.
    /// </summary>
    public static bool TopicRouteData2EndpointsForStaticTopic(string topic,
        TopicRouteData route, Dictionary<MessageQueue, string> outEndpoints)
    {
        outEndpoints.Clear();
        if (route.TopicQueueMappingByBroker.IsNull || !route.TopicQueueMappingByBroker.IsObject
            || route.TopicQueueMappingByBroker.ObjectItems().Count == 0)
        {
            return false;
        }

        var infosByScope = new Dictionary<string, List<KeyValuePair<string, TopicQueueMappingInfo>>>(
            StringComparer.Ordinal);
        foreach (var entry in route.TopicQueueMappingByBroker.ObjectItems())
        {
            if (!ParseTopicQueueMappingInfo(entry.Value, out TopicQueueMappingInfo? info))
            {
                continue;
            }

            if (!infosByScope.TryGetValue(info.Scope, out var list))
            {
                list = new List<KeyValuePair<string, TopicQueueMappingInfo>>();
                infosByScope[info.Scope] = list;
            }

            list.Add(new KeyValuePair<string, TopicQueueMappingInfo>(entry.Key, info));
        }

        var endpoints = new Dictionary<MessageQueue, string>();
        foreach (var scopeEntry in infosByScope)
        {
            string scope = scopeEntry.Key;
            List<KeyValuePair<string, TopicQueueMappingInfo>> infos = scopeEntry.Value;
            infos.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
            infos.Sort((a, b) => b.Value.Epoch.CompareTo(a.Value.Epoch));

            int maxTotalNums = 0;
            const long maxTotalNumOfEpoch = -1;
            string mockName = GetMockBrokerName(scope);
            var mqEndPoints = new Dictionary<MessageQueue, (long Epoch, string Bname)>();
            foreach (var ni in infos)
            {
                TopicQueueMappingInfo info = ni.Value;
                if (info.Epoch >= maxTotalNumOfEpoch && info.TotalQueues > maxTotalNums)
                {
                    maxTotalNums = info.TotalQueues;
                }

                // Only the global id (the key) shape determines the endpoint set; the physical id is broker-internal.
                foreach (int globalId in info.CurrIdMap.Keys)
                {
                    var mq = new MessageQueue(topic, mockName, globalId);
                    if (!mqEndPoints.TryGetValue(mq, out (long Epoch, string Bname) old)
                        || old.Epoch <= info.Epoch)
                    {
                        mqEndPoints[mq] = (info.Epoch, info.Bname);
                    }
                }
            }

            // Fill in the static logical queues: every id below total exists; those with no host point to the
            // NOT_EXIST sentinel.
            for (int i = 0; i < maxTotalNums; i++)
            {
                var mq = new MessageQueue(topic, mockName, i);
                endpoints[mq] = mqEndPoints.TryGetValue(mq, out (long _, string bname) holder)
                    ? holder.bname
                    : LogicalQueueMockNotExist;
            }
        }

        if (endpoints.Count == 0)
        {
            return false;
        }

        foreach (var kv in endpoints)
        {
            outEndpoints[kv.Key] = kv.Value;
        }

        return true;
    }
}
