using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

// 测试工程与本程序集没有 InternalsVisibleTo 关系，故用 public，勿用于业务代码
// （业务侧入口是 MqClient.GetBrokerNameFromMessageQueue / PopAckAddrFor）。
public static class StaticTopic
{
    // MixAll.METADATA_SCOPE_GLOBAL / LOGICAL_QUEUE_MOCK_BROKER_PREFIX /
    // LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST。
    public const string MetadataScopeGlobal = "__global__";
    public const string LogicalQueueMockBrokerPrefix = "__syslo__";
    public const string LogicalQueueMockNotExist = "__syslo__none__";

    public static bool IsLogicalQueueMockBrokerName(string brokerName) =>
        brokerName.StartsWith(LogicalQueueMockBrokerPrefix, StringComparison.Ordinal);

    /// <summary>
    /// getMockBrokerName："__global__"（scope 本身）剥掉头两个字符，其余 scope 原样加前缀。
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
        // globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
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
                // 键是 globalId 的十进制字符串；解析失败的条目跳过。
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
    /// ClientMetadata.topicRouteData2EndpointsForStaticTopic：构建逻辑队列集
    /// MessageQueue(topic, mockBrokerName(scope), globalId) -> 承载它的真实 broker 名
    /// （total 范围内没有承载者的逻辑 id 指向 "__syslo__none__"）。
    /// 空映射（非静态主题）返回 false 且不动 out。
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

                // 只有 global id（键）形状决定端点集；物理 id 是 broker 内部的。
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

            // 补齐静态逻辑队列：total 之下的每个 id 都存在；没有承载者的指向
            // NOT_EXIST 哨兵。
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
