// 静态主题（单元化主题）消费者侧路由支持，移植自 Java 客户端：
//   - org.apache.rocketmq.remoting.rpc.ClientMetadata
//     .topicRouteData2EndpointsForStaticTopic
//   - org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils
//     .getMockBrokerName
//   - org.apache.rocketmq.client.impl.factory.MQClientInstance
//     .getBrokerNameFromMessageQueue（见 MqClient 同名方法）
//
// wire 事实（不得"简化"）：
//   - 路由解码侧把 TopicQueueMappingByBroker 保持为不透明 JsonValue；类型化视图
//     在这里解析，不动协议层透传。
//   - TopicQueueMappingInfo.scope 缺省为 MixAll.METADATA_SCOPE_GLOBAL
//     （"__global__"）；global scope 的 mock broker 名会剥掉开头两个下划线
//     （"__syslo__global__"）。
//   - Java 的 maxTotalNumOfEpoch 读了但从不赋值（恒 -1），所以
//     epoch >= maxTotalNumOfEpoch 守卫恒真，maxTotalNums 就是该 scope 的
//     totalQueues 最大值 —— 这个 quirk 照抄。
//   - 同 epoch 的后到条目覆盖先到条目（Java `oldEpoch <= epoch`）。
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

    /// <summary>Java MixAll::isLogicalQueueMockBrokerName（前缀判断）。</summary>
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

    /// <summary>topicQueueMappingByBroker[brokerName] 单条目的类型化视图
    /// （Java TopicQueueMappingInfo）。</summary>
    public sealed class TopicQueueMappingInfo
    {
        public string Scope = MetadataScopeGlobal;
        public int TotalQueues;
        public string Bname = string.Empty;
        public long Epoch;
        // globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
        public readonly SortedDictionary<int, int> CurrIdMap = new();
    }

    /// <summary>
    /// 从路由解码侧保留下来的原始 json 对象里解析类型化视图。
    /// 畸形条目返回 false —— Java 遇到会 NPE；能抢救的部分映射好过整个崩溃。
    /// </summary>
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
                // 键是 globalId 的十进制字符串；解析失败的条目跳过（同 Go/Python 口径）。
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

        // 按 scope 分组（Java 里没有 scope 的条目不参与，同此）。
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
            // 先按 broker 名排序，再做稳定的 epoch 降序排序。Java 对
            // ConcurrentHashMap 的 entrySet 做 epoch 降序排序，同 epoch 的覆盖顺序本就
            // 未定义；钉死 broker 升序让结果可复现。
            infos.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
            infos.Sort((a, b) => b.Value.Epoch.CompareTo(a.Value.Epoch));

            int maxTotalNums = 0;
            // Java 从不更新 maxTotalNumOfEpoch —— 守卫恒真，照抄。
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
