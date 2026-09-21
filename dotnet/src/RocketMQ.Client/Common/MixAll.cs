// org.apache.rocketmq.common.MixAll 的 C# 对应：全局常量与静态工具。
namespace RocketMQ.Common;

public static class MixAll
{
    public const string NamesrvAddrProperty = "rocketmq.namesrv.addr";
    public const string NamesrvAddrEnv = "NAMESRV_ADDR";
    public const string MessageCompressLevel = "rocketmq.message.compressLevel";
    public const string DefaultTopic = "TBW102";
    public const string BenchmarkTopic = "BenchmarkTest";
    public const string DefaultProducerGroup = "DEFAULT_PRODUCER";
    /// <summary>Java <c>ClientConfig#instanceName</c> 的默认值（<c>System.getProperty("rocketmq.client.name", "DEFAULT")</c>）。</summary>
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

    // Request-Reply（5.x）：应答 topic = <cluster>_REPLY_TOPIC（Java MixAll.REPLY_TOPIC_POSTFIX）。
    // 注意：它**不是**上面那个老的控制台前缀 "%REPLY%"，两者语义完全不同——
    // GetReplyTopic 返回的是集群级系统 topic（broker 预注册，客户端切勿自建）。
    public const string REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";

    // 应答消息的 MSG_TYPE 属性值（Java MixAll.REPLY_MESSAGE_FLAG）。大小写敏感。
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

    // ⚠ Java 的 MixAll.UNIQUE_MSG_QUERY_FLAG 是 extFields 里的**键名**（取值 "true"/"false"），
    // 不是一个整数标志位。早期 C++ 实现把它当 1 用是错的：发给 broker 后 key 变成数字键，
    // uniqKey 查询路由不到 RocksDB 索引分支。按 key 查消息的三种模式见 QueryMsgType。
    public const string UniqueMsgQueryFlag = "_UNIQUE_KEY_QUERY";
    public const string ScheduleTopic = "SCHEDULE_TOPIC_XXXX";
    public const string LmqPrefix = "%LMQ%";
    public const int LmqQueueId = 0;
    public const string TraceTopic = "RMQ_SYS_TRACE_TOPIC";
    public const string RealTraceTopic = "rmq_sys_TRACE_DATA";
    /// <summary>轨迹 region 缺省值（对应 Java MixAll.DEFAULT_TRACE_REGION_ID）。</summary>
    public const string DefaultTraceRegionId = "DefaultRegion";
    public const string RmqSysTransHalfTopic = "RMQ_SYS_TRANS_HALF_TOPIC";
    public const string RmqSysTransOpHalfTopic = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public const int TransCheckMaxTime = 15;
    public const string UnitPrefix = "unit_";

    /// <summary>
    /// 对应 Java <c>MixAll.REQ_T</c>：请求类型标记的 ExtFields 键，由 stream 请求钩子
    /// （<c>StreamTypeRPCHook</c>）写入，取值是 <c>RequestType.STREAM.getCode()</c>
    /// 即 <c>"0"</c>（不是枚举名 —— clientId 的后缀才用枚举名 <c>@STREAM</c>）。
    /// </summary>
    public const string ReqT = "ReqT";

    public const int DefaultTopicQueueNums = 4;
    public const int DefaultTopicReadQueueNums = 4;
    public const int DefaultTopicWriteQueueNums = 4;
    public const int MaxTopicLength = 127;
    public const int MaxGroupLength = 255;
    public const int CharacterMaxLength = 255;

    public const int MasterId = 0;
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

    /// <summary>
    /// 应答 topic = &lt;cluster&gt;_REPLY_TOPIC（Java MixAll.getReplyTopic）。
    /// 对应 Python 的 <c>cluster_name + "_" + REPLY_TOPIC_POSTFIX</c>。
    /// broker 预注册该系统 topic，客户端只发不建。
    /// </summary>
    public static string GetReplyTopic(string clusterName) => clusterName + "_" + REPLY_TOPIC_POSTFIX;

    public static bool IsSysTopic(string topic) => topic.StartsWith(SystemTopicPrefix, StringComparison.Ordinal);

    /// <summary>对应 Java MixAll.isLmq（LMQ topic 以 %LMQ% 开头）。</summary>
    public static bool IsLmq(string lmqMetaData) =>
        lmqMetaData.StartsWith(LmqPrefix, StringComparison.Ordinal);

    /// <summary>对应 Java MixAll.isSysConsumerGroup（CID_RMQ_SYS_ 前缀）。</summary>
    public static bool IsSysConsumerGroup(string consumerGroup) =>
        consumerGroup.StartsWith(CidRmqSysPrefix, StringComparison.Ordinal);

    /// <summary>对应 Java MixAll.isPredefinedGroup（PREDEFINE_GROUP_SET 命中）。</summary>
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

    /// <summary>本机出口 IP（UDP connect 探测，无外网回落 hostname -> 127.0.0.1）。</summary>
    public static string GetIpStr() => UtilAll.LocalIp();

    /// <summary>
    /// 进程内只探测一次的本机 IP。对应 Java <c>ClientConfig#clientIP</c>：它在 ClientConfig
    /// 构造时就定下来，同一个客户端的 clientId 因此稳定（每次重新探测既慢，又可能在换网卡
    /// 后让重启的客户端换一个 clientId）。
    /// </summary>
    public static string CachedIpStr() => _cachedIpStr ??= UtilAll.LocalIp();

    private static string? _cachedIpStr;

    public static int Pid() => UtilAll.Pid();

    // ---------------- Properties <-> String ----------------
    // 对应 Java MixAll.properties2String / string2Properties。
    //
    // 这是管理端最容易踩的坑：**GET_BROKER_CONFIG 的响应体是 properties 文本**
    // （每行 "key=value"），不是 JSON、也不是 KVTable。用 JSON 解析器去解必然失败。
    public static string Properties2String(PropertyMap properties)
    {
        // Java: 每条 "key=value\n"（Properties.store 风格用 "=" 作分隔符），null 值跳过。
        // 这里 PropertyMap 的值是 string，没有 null 概念，故不做过滤；
        // 调用方若想跳过某键，不要放进 map 即可。
        var sb = new System.Text.StringBuilder();
        foreach (var kv in properties)
        {
            sb.Append(kv.Key).Append('=').Append(kv.Value).Append('\n');
        }

        return sb.ToString();
    }

    public static PropertyMap String2Properties(string text)
    {
        // 语义对齐 Java MixAll.string2Properties -> java.util.Properties.load：
        //   1) 行尾未转义的 '\' 表示续行（下一行前导空白被丢弃）；
        //   2) 空行、以 '#' 或 '!' 开头的行是注释；
        //   3) 键与值以**第一个** '='、':' 或**空白**分隔（空白也是合法分隔符！）；
        //   4) 分隔符前后的空白被跳过；值的**尾部**空白保留（Java 不去尾空白）。
        //
        // 注：Java 还会处理 \t \n \uXXXX 等转义，broker 配置导出里不出现，
        // 这里不实现（把反斜杠语义做错反而更不一致）。
        var result = new PropertyMap();

        // 先把续行合并成逻辑行
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

            // 行尾反斜杠个数为奇数 => 续行
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
            // 跳过分隔符前的空白
            while (i < n && IsSpaceChar(line[i]))
            {
                i++;
            }

            // 可选的 '=' / ':' 及其后的空白
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

    /// <summary>按 \r\n / \n / \r 切行（等价于 Python str.splitlines 对本场景的行为）。</summary>
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

/// <summary>
/// 对应 Java QueryMsgByKeySubCommand.QueryMsgType：按 key 查消息的三种模式。
/// 与 MixAll.UniqueMsgQueryFlag（extFields 键名）不是一回事。
/// </summary>
public static class QueryMsgType
{
    public const int AllMessage = 0;
    public const int UniqueKey = 1;
    public const int Normal = 2;
}
