using System.Globalization;

namespace RocketMQ.Common;

public static class MixAll
{
    public const string NamesrvAddrProperty = "rocketmq.namesrv.addr";
    public const string NamesrvAddrEnv = "NAMESRV_ADDR";
    public const string MessageCompressLevel = "rocketmq.message.compressLevel";
    public const string DefaultTopic = "TBW102";
    public const string BenchmarkTopic = "BenchmarkTest";
    public const string DefaultProducerGroup = "DEFAULT_PRODUCER";
    public const string DefaultInstanceName = "DEFAULT";
    public const string DefaultConsumerGroup = "DEFAULT_CONSUMER";
    public const string ClientInnerProducerGroup = "CLIENT_INNER_PRODUCER";
    public const string SelfTestProducerGroup = "SELF_TEST_P_GROUP";
    public const string SelfTestConsumerGroup = "SELF_TEST_C_GROUP";
    public const string OnsAddr = "ONS_ADDR";
    public const string CidRmqSysPrefix = "CID_RMQ_SYS_";
    public const string CidOnsapiPrefix = "CID_ONSAPI_";
    public const string ProxyName = "MQProxy";

    public const string RetryGroupTopicPrefix = "%RETRY%";
    public const string DlqGroupTopicPrefix = "%DLQ%";
    public const string ReplyTopicPrefix = "%REPLY%";

    public const string REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";

    public const string REPLY_MESSAGE_FLAG = "reply";

    public const string SystemTopicPrefix = "rmq_sys_";
    public const string ToolsConsumerGroup = "TOOLS_CONSUMER";
    public const string FiltersrvConsumerGroup = "FILTERSRV_CONSUMER";
    public const string MonitorConsumerGroup = "__MONITOR_CONSUMER";
    public const string ClientInnerConsumerGroup = "CLIENT_INNER_CONSUMER";
    public const string OnsNamespace = "namespace";
    public const string ScheduleConsumerGroup = "SCHEDULE_CONSUMER";
    public const string OnsHttpProxyGroup = "CID_ONSHTTP_PROXY";
    public const string CidOnsapiPermissionGroup = "CID_ONSAPI_PERMISSION";
    public const string CidOnsapiOwnerGroup = "CID_ONSAPI_OWNER";
    public const string CidOnsapiPullGroup = "CID_ONSAPI_PULL";
    public const string CidSysRmqTrans = "CID_SYS_RMQ_TRANS";

    public const string UniqueMsgQueryFlag = "_UNIQUE_KEY_QUERY";
    public const string ScheduleTopic = "SCHEDULE_TOPIC_XXXX";
    public const string LmqPrefix = "%LMQ%";
    public const int LmqQueueId = 0;
    public const string TraceTopic = "RMQ_SYS_TRACE_TOPIC";
    public const string RealTraceTopic = "rmq_sys_TRACE_DATA";
    public const string DefaultTraceRegionId = "DefaultRegion";
    public const string RmqSysTransHalfTopic = "RMQ_SYS_TRANS_HALF_TOPIC";
    public const string RmqSysTransOpHalfTopic = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public const int TransCheckMaxTime = 15;
    public const string UnitPrefix = "unit_";

    public const string NamespaceOrderTopicConfig = "ORDER_TOPIC_CONFIG";

    public const string ReqT = "ReqT";

    public const string RpcNamespacedField = "nsd";

    public const string RpcNamespaceField = "ns";

    public const int DefaultTopicQueueNums = 4;
    public const int DefaultTopicReadQueueNums = 4;
    public const int DefaultTopicWriteQueueNums = 4;
    public const int MaxTopicLength = 127;
    public const int MaxGroupLength = 255;
    public const int CharacterMaxLength = 255;

    public const int MasterId = 0;

    // Heartbeat V2: the two flags a V2-capable broker stamps on its HEART_BEAT(34)
    // response. Both travel as the TEXT "true"/"false", so a response that carries
    // neither (every pre-V2 broker) reads as "not V2-capable" -- the decoding rule this
    // client applies when it stamps and reads the two flags.
    public const string IsSupportHeartbeatV2 = "IS_SUPPORT_HEART_BEAT_V2";
    public const string IsSubChange = "IS_SUB_CHANGE";
    public const int ReadPermByDefault = 4 | 2; // PERM_READ | PERM_WRITE

    private static readonly string[] PredefineGroups =
    {
        DefaultConsumerGroup,
        DefaultProducerGroup,
        ToolsConsumerGroup,
        ScheduleConsumerGroup,
        FiltersrvConsumerGroup,
        MonitorConsumerGroup,
        ClientInnerProducerGroup,
        SelfTestProducerGroup,
        SelfTestConsumerGroup,
        OnsHttpProxyGroup,
        CidOnsapiPermissionGroup,
        CidOnsapiOwnerGroup,
        CidOnsapiPullGroup,
        CidSysRmqTrans,
    };

    public static string GetRetryTopic(string consumerGroup) => RetryGroupTopicPrefix + consumerGroup;

    public static bool IsRetryTopic(string topic) => topic.StartsWith(RetryGroupTopicPrefix, StringComparison.Ordinal);

    public static string GetDlqTopic(string consumerGroup) => DlqGroupTopicPrefix + consumerGroup;

    public static bool IsDlqTopic(string topic) => topic.StartsWith(DlqGroupTopicPrefix, StringComparison.Ordinal);

    public static string BrokerVipChannel(bool isChange, string brokerAddr)
    {
        if (!isChange) return brokerAddr;
        int index = brokerAddr.LastIndexOf(':');
        if (index <= 0) return brokerAddr;
        string host = brokerAddr[..index];
        string port = brokerAddr[(index + 1)..];
        if (!long.TryParse(port, CultureInfo.InvariantCulture, out long portNum)) return brokerAddr;
        return host + ":" + (portNum - 2).ToString(CultureInfo.InvariantCulture);
    }

    public static string GetReplyTopic(string clusterName) => clusterName + "_" + REPLY_TOPIC_POSTFIX;

    public static bool IsSysTopic(string topic) => topic.StartsWith(SystemTopicPrefix, StringComparison.Ordinal);

    public static bool IsLmq(string lmqMetaData) =>
        lmqMetaData.StartsWith(LmqPrefix, StringComparison.Ordinal);

    public static bool IsSysConsumerGroup(string consumerGroup) =>
        consumerGroup.StartsWith(CidRmqSysPrefix, StringComparison.Ordinal);

    public static bool IsPredefinedGroup(string consumerGroup)
    {
        foreach (string g in PredefineGroups)
        {
            if (consumerGroup == g)
            {
                return true;
            }
        }

        return false;
    }

    public static string ResetRetryAndDlqTopic(string topic)
    {
        if (IsRetryTopic(topic))
        {
            return topic[RetryGroupTopicPrefix.Length..];
        }

        if (IsDlqTopic(topic))
        {
            return topic[DlqGroupTopicPrefix.Length..];
        }

        return topic;
    }

    /// <summary>The local outbound IP (UDP connect probe; falls back to hostname -> 127.0.0.1 when no external network).</summary>
    public static string GetIpStr() => UtilAll.LocalIp();

    public static string CachedIpStr() => _cachedIpStr ??= UtilAll.LocalIp();

    private static string? _cachedIpStr;

    public static int Pid() => UtilAll.Pid();

    // This is the easiest pitfall for the admin side: **the GET_BROKER_CONFIG response body is properties text**
    // (each line "key=value"), not JSON and not a KVTable. Parsing it with a JSON parser is bound to fail.
    public static string Properties2String(PropertyMap properties)
    {
        var sb = new System.Text.StringBuilder();
        foreach (var kv in properties)
        {
            sb.Append(kv.Key).Append('=').Append(kv.Value).Append('\n');
        }

        return sb.ToString();
    }

    public static PropertyMap String2Properties(string text)
    {
        var result = new PropertyMap();

        // First merge continuation lines into logical lines
        var logical = new List<string>();
        string pending = string.Empty;
        bool hasPending = false;
        foreach (string raw in SplitLines(text))
        {
            string line = raw;
            if (hasPending)
            {
                line = pending + TrimLeft(line);
                pending = string.Empty;
                hasPending = false;
            }

            // odd number of trailing backslashes => continuation line
            int trailing = 0;
            for (int i = line.Length; i > 0 && line[i - 1] == '\\'; i--)
            {
                trailing++;
            }

            if (trailing % 2 == 1)
            {
                pending = line[..^1];
                hasPending = true;
                continue;
            }

            logical.Add(line);
        }

        if (hasPending)
        {
            logical.Add(pending);
        }

        foreach (string line in logical)
        {
            string stripped = TrimBoth(line);
            if (stripped.Length == 0)
            {
                continue;
            }

            if (stripped[0] == '#' || stripped[0] == '!')
            {
                continue;
            }

            int n = line.Length;
            int i = 0;
            while (i < n && IsSpaceChar(line[i]))
            {
                i++;
            }

            int keyStart = i;
            while (i < n && line[i] != '=' && line[i] != ':' && !IsSpaceChar(line[i]))
            {
                i++;
            }

            string key = line[keyStart..i];
            // Skip whitespace before the separator
            while (i < n && IsSpaceChar(line[i]))
            {
                i++;
            }

            // Optional '=' / ':' and the whitespace after it
            if (i < n && (line[i] == '=' || line[i] == ':'))
            {
                i++;
                while (i < n && IsSpaceChar(line[i]))
                {
                    i++;
                }
            }

            result[key] = line[i..];
        }

        return result;
    }

    private static bool IsSpaceChar(char c) => c == ' ' || c == '\t' || c == '\f';

    private static string TrimLeft(string s)
    {
        int i = 0;
        while (i < s.Length && IsSpaceChar(s[i]))
        {
            i++;
        }

        return s[i..];
    }

    private static string TrimBoth(string s)
    {
        int b = 0;
        while (b < s.Length && IsSpaceChar(s[b]))
        {
            b++;
        }

        int e = s.Length;
        while (e > b && IsSpaceChar(s[e - 1]))
        {
            e--;
        }

        return s[b..e];
    }

    /// <summary>Split lines by \r\n / \n / \r.</summary>
    private static List<string> SplitLines(string text)
    {
        var lines = new List<string>();
        var cur = new System.Text.StringBuilder();
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (c == '\n')
            {
                lines.Add(cur.ToString());
                cur.Clear();
            }
            else if (c == '\r')
            {
                lines.Add(cur.ToString());
                cur.Clear();
                if (i + 1 < text.Length && text[i + 1] == '\n')
                {
                    i++;
                }
            }
            else
            {
                cur.Append(c);
            }
        }

        if (cur.Length > 0)
        {
            lines.Add(cur.ToString());
        }

        return lines;
    }
}

public static class QueryMsgType
{
    public const int AllMessage = 0;
    public const int UniqueKey = 1;
    public const int Normal = 2;
}
