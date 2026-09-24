// 管理客户端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt /
// MQAdminExt 与 org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl）。
//
// 覆盖：Topic 增删查/配置、Broker 集群信息与运行时信息/配置、NameServer KV 配置、
// 订阅组管理、消费者/生产者连接、消费统计、offset 管理（含真实 broker 位点重置）、
// 消息查询（key / uniqKey / msgId / ConsumeQueue）。
//
// 与 C++ 实现（cpp/src/client/admin.cpp）逐项对齐。对齐要点（Java 5.x 探针 +
// 源码核对结论，勿凭记忆改）：
//  - GET_BROKER_CONFIG 的 body 是 **properties 文本**（"k=v\n"），不是 JSON。
//  - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP 的 body 是 SubscriptionGroupConfig JSON。
//  - GET_TOPIC_CONFIG 请求头带 topic + lo，响应体是 TopicConfig JSON。
//  - GET_ALL_SUBSCRIPTIONGROUP_CONFIG 是**分页**接口（groupSeq / maxGroupNum / dataVersion）。
//  - ResetOffsetBody.offsetTable 是 Map<MessageQueue, Long>。
//  - KV 配置类请求打到 **NameServer**，且 PUT/DELETE 要广播到**每一个** NameServer。
//  - INVOKE_BROKER_TO_RESET_OFFSET 必须覆盖 language 为 CPP，broker 才会回可解析的 offsetTable。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>
/// 对应 C++ DefaultMQAdminExt（org.apache.rocketmq.client.admin.DefaultMQAdminExt）。
/// 管理端对外操作的统一入口：内部持有 <see cref="MQClientInstance"/> 复用其底层调用。
/// </summary>
public sealed class DefaultMQAdminExt
{
    // 对应 DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3
    public const int DefaultTimeout = 5000 * 3;

    private string _instanceName;
    private string _clientId = string.Empty;
    private List<string> _nameServerAddrs = new();
    private readonly List<string> _kvNamespaceToDeleteList = new();
    private int _timeoutMillis = DefaultTimeout;

    private MQClientInstance? _mqClient;
    private bool _started;

    public DefaultMQAdminExt(string instanceName = "ADMIN")
    {
        _instanceName = instanceName;
    }

    // ---------------------------------------------------------------- 配置与生命周期

    /// <summary>分号分隔的 namesrv 地址列表（对应 Java / C++ 的 setNamesrvAddr）。</summary>
    public void SetNamesrvAddr(string addr)
    {
        _nameServerAddrs.Clear();
        int start = 0;
        while (start <= addr.Length)
        {
            int semi = addr.IndexOf(';', start);
            string piece = semi < 0 ? addr[start..] : addr[start..semi];
            // 去首尾空白（C++ 只去 ' ' / '\t'，此处用 Trim 等价且更宽松）
            piece = piece.Trim();
            if (piece.Length > 0) _nameServerAddrs.Add(piece);
            if (semi < 0) break;
            start = semi + 1;
        }
    }

    public void SetNameServerAddresses(IReadOnlyList<string> addrs) =>
        _nameServerAddrs = new List<string>(addrs);

    /// <summary>对应 C++ setInstanceName（运行期改 instanceName 不会重建 client）。</summary>
    public void SetInstanceName(string name) => _instanceName = name;

    /// <summary>instanceName 会被 Start() 就地改写，所以这里读的是启动后的实际值。</summary>
    public string GetInstanceName() => _instanceName;

    /// <summary>Start() 之后才有值（对应其他 facade 的 ClientId）。</summary>
    public string ClientId => _clientId;

    public string GetNamesrvAddr()
    {
        var sb = new StringBuilder();
        for (int i = 0; i < _nameServerAddrs.Count; ++i)
        {
            if (i > 0) sb.Append(';');
            sb.Append(_nameServerAddrs[i]);
        }

        return sb.ToString();
    }

    public List<string> GetNameServerAddressList() => new List<string>(_nameServerAddrs);

    // ---------------- unitName / enableStreamRequestType ----------------
    // 对应 Java ClientConfig 的同名开关（admin 只用到 unitName：clientId 与动态取址 URL；
    // Java 的 DefaultMQAdminExt 从不置 unitMode / stream，默认全关）。
    /// <summary>单元名：进 clientId 的 <c>@&lt;unitName&gt;</c> 段，也拼进动态取址 URL。</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>true 时每笔请求带 <c>ReqT=0</c>、clientId 末尾多一段 <c>@STREAM</c>。</summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    /// <summary>
    /// Java <c>ClientConfig#pollNameServerInterval</c>（:58，默认 30000ms）：在用 topic 的
    /// 路由刷新周期，Start() 时透传给 MQClientInstance（之后改不重排已启动的周期任务）。
    /// </summary>
    public int PollNameServerIntervalMillis
    {
        get => _pollNameServerIntervalMillis;
        set => _pollNameServerIntervalMillis = value;
    }

    // ---------------- ACL 鉴权（对应 Java DefaultMQAdminExt(rpcHook)）----------------
    // 必须在 Start() 之前调用。
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>便捷入口：用 accessKey/secretKey（可选 securityToken）构造 AclClientRPCHook。</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ACL 钩子，Start() 时绑定到 MQClientInstance 的传输层
    private IRpcHook? _rpcHook;
    private string _unitName = string.Empty;
    private bool _enableStreamRequestType;
    // Java ClientConfig#pollNameServerInterval 的默认值（:58）
    private int _pollNameServerIntervalMillis = 30000;

    public void SetTimeoutMillis(int millis) => _timeoutMillis = millis;

    public int GetTimeoutMillis() => _timeoutMillis;

    /// <summary>删除 topic 时一并清理的 KV namespace（对应 Java kvNamespaceToDeleteList）。</summary>
    public void AddKvNamespaceToDeleteList(string ns) => _kvNamespaceToDeleteList.Add(ns);

    public void Start()
    {
        if (_started) return;
        if (_nameServerAddrs.Count == 0)
        {
            throw new MQClientException("name server address is not set");
        }

        // Java `DefaultMQAdminExtImpl#start`:161 无条件 `changeInstanceNameToPID`，
        // clientId 再走 `ClientConfig#buildMQClientId` 的 `<本机 IP>@<instanceName>`。
        // 本端口的 admin 默认 instanceName 是 "ADMIN"（不是 Java 的 "DEFAULT"，因为 admin
        // 用私有实例、不和其他客户端共用），所以这一步只在调用方显式设成 "DEFAULT" 时才起作用。
        _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
        if (string.IsNullOrEmpty(_clientId))
        {
            _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
        }

        // 请求钩子（ACL 签名 / stream 的 ReqT）：绑定在 Start() **之前**（Java 的 rpcHook
        // 随 MQClientAPIImpl 构造传入，实例第一笔报文就带着它）。
        // admin 在 Java 里既不置 unitMode 也不置 stream，默认全关。
        IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook);
        _mqClient = new MQClientInstance(_clientId, _nameServerAddrs, unitName: _unitName,
            pollNameServerIntervalMillis: _pollNameServerIntervalMillis);
        if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
        {
            ClientLog.Warn("admin rpc hook ignored: MQClientInstance already has one (clientId="
                + _clientId + ")");
        }
        _mqClient.Start();

        _started = true;
    }

    public void Shutdown()
    {
        if (!_started) return;
        _started = false;
        _mqClient?.Shutdown();
    }

    public bool IsStarted => _started;

    private MQClientInstance RequireClient()
    {
        if (!_started || _mqClient is null)
        {
            throw new MQClientException("admin not started, call start() first");
        }

        return _mqClient;
    }

    public MQClientInstance GetMQClientInstance() => RequireClient();

    /// <summary>对应 C++ client()（GetMQClientInstance 的别名）。</summary>
    public MQClientInstance Client() => GetMQClientInstance();

    // ---------------------------------------------------------------- 内部工具

    private static string FirstBrokerAddr(MQClientInstance client)
    {
        ClusterInfo ci = client.GetBrokerClusterInfo();
        List<string> addrs = ci.GetBrokerAddrs();
        if (addrs.Count == 0) throw new MQClientException("no broker address available");
        return addrs[0];
    }

    private static string FindFirstBrokerAddr(MQClientInstance client) => FirstBrokerAddr(client);

    private static string BrokerAddrForMq(MQClientInstance client, MessageQueue mq) =>
        client.BrokerAddrForMq(mq);

    private static List<string> BrokerAddrsOfCluster(MQClientInstance client, string clusterName)
    {
        ClusterInfo ci = client.GetBrokerClusterInfo();
        if (clusterName.Length == 0) return ci.GetBrokerAddrs();
        List<string> addrs = ci.GetBrokerAddrsOfCluster(clusterName);
        if (addrs.Count == 0) return ci.GetBrokerAddrs();
        return addrs;
    }

    /// <summary>把 int 写进 extFields（Java 侧 extFields 全是字符串）。</summary>
    private static string I64Str(long v) => v.ToString(CultureInfo.InvariantCulture);

    /// <summary>取响应 extFields 中的整数（缺失/空/非法返回 fallback）。</summary>
    private static long ExtInt(RemotingCommand r, string key, long fallback)
    {
        if (r.ExtFields.TryGetValue(key, out string? v) && !string.IsNullOrEmpty(v))
        {
            return long.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out long n)
                ? n
                : fallback;
        }

        return fallback;
    }

    /// <summary>JsonValue 的等价文本（用于比较 DataVersion 是否变化）。</summary>
    private static string DvText(JsonValue v) => v.IsNull ? "null" : v.Dump();

    // ---------------------------------------------------------------- Topic 管理

    // 对应 DefaultMQAdminExt.createTopic：key / topicSysFlag 在 C++ 实作中被忽略，
    // 直接走 createTopicInRoute（默认 TopicConfig defaultTopic=TBW102，perm=READ|WRITE）。
    public void CreateTopic(string key, string newTopic, int queueNum = 4, int topicSysFlag = 0)
    {
        // 对应 Java DefaultMQProducerImpl.createTopic 的本地校验：非法/系统 topic 名不该打到 broker
        Validators.CheckTopic(newTopic);
        Validators.IsSystemTopic(newTopic);
        RequireClient().CreateTopicInRoute(newTopic, queueNum, queueNum, MixAll.ReadPermByDefault);
    }

    public void CreateAndUpdateTopicConfig(string addr, TopicConfig config) =>
        RequireClient().CreateTopicInBroker(
            addr, MixAll.DefaultTopic, config.TopicName, config.ReadQueueNums,
            config.WriteQueueNums, config.Perm, config.TopicSysFlag,
            config.TopicFilterType, MixAll.Properties2String(config.Attributes));

    public void CreateTopicInBroker(string brokerAddr, string topic,
        int readQueueNums = 4, int writeQueueNums = 4, int perm = 6) =>
        RequireClient().CreateTopicInBroker(
            brokerAddr, MixAll.DefaultTopic, topic, readQueueNums, writeQueueNums, perm);

    public void DeleteTopicInBroker(string brokerAddr, string topic) =>
        RequireClient().DeleteTopicInBroker(brokerAddr, topic);

    public void DeleteTopicInNameServer(List<string> addrs, string topic)
    {
        MQClientInstance client = RequireClient();
        List<string> targets = addrs.Count == 0 ? new List<string>(client.NameServerAddrs) : addrs;
        foreach (string ns in targets)
        {
            PropertyMap ext = new() { ["topic"] = topic };
            client.InvokeSync(ns, RequestCode.DeleteTopicInNamesrv, ext, null, false, _timeoutMillis);
        }
    }

    public void DeleteTopicInNamesrv(string topic) => RequireClient().DeleteTopicInNamesrv(topic);

    public void DeleteTopic(string topic, string clusterName = "")
    {
        MQClientInstance client = RequireClient();
        foreach (string broker in BrokerAddrsOfCluster(client, clusterName))
        {
            try
            {
                client.DeleteTopicInBroker(broker, topic);
            }
            catch (Exception e)
            {
                ClientLog.Warn("delete topic " + topic + " in broker " + broker + " failed: " + e.Message);
            }
        }

        try
        {
            client.DeleteTopicInNamesrv(topic);
        }
        catch (Exception e)
        {
            ClientLog.Warn("delete topic " + topic + " in name server failed: " + e.Message);
        }

        foreach (string ns in _kvNamespaceToDeleteList)
        {
            try
            {
                DeleteKvConfig(ns, topic);
            }
            catch (Exception e)
            {
                ClientLog.Warn("delete kv config " + ns + "/" + topic + " failed: " + e.Message);
            }
        }
    }

    public TopicList FetchAllTopicList() => RequireClient().GetAllTopicListFromNameServer();

    public HashSet<string> FetchTopicsByCluster(string clusterName)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new() { ["clusterName"] = clusterName };
        var topics = new HashSet<string>();
        string lastError = string.Empty;
        foreach (string ns in client.NameServerAddrs)
        {
            try
            {
                RemotingCommand response = client.InvokeSync(
                    ns, RequestCode.GetTopicsByCluster, ext, null, false, _timeoutMillis);
                if (response.Body.Length > 0)
                {
                    if (RemotingSerializable.Decode(response.Body, out JsonValue v))
                    {
                        JsonValue? arr = v.Find("topicList");
                        if (arr is { IsArray: true })
                        {
                            for (int i = 0; i < arr.Size(); ++i)
                            {
                                if (arr.At(i).IsString) topics.Add(arr.At(i).StringValue());
                            }
                        }
                    }
                }

                return topics;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("all name servers unreachable: " + lastError);
    }

    public HashSet<string> GetClusterList(string topic)
    {
        MQClientInstance client = RequireClient();
        ClusterInfo ci = client.GetBrokerClusterInfo();
        TopicRouteData route = ExamineTopicRoute(topic);
        var brokerNames = new HashSet<string>();
        foreach (BrokerData bd in route.BrokerDatas) brokerNames.Add(bd.BrokerName);

        var clusters = new HashSet<string>();
        foreach (var kv in ci.ClusterAddrTable)
        {
            foreach (string n in kv.Value)
            {
                if (brokerNames.Contains(n))
                {
                    clusters.Add(kv.Key);
                    break;
                }
            }
        }

        return clusters;
    }

    public List<TopicRouteData> FetchAllTopicRoute()
    {
        MQClientInstance client = RequireClient();
        var result = new List<TopicRouteData>();
        TopicList topics = client.GetAllTopicListFromNameServer();
        foreach (string topic in topics.Topics)
        {
            try
            {
                TopicRouteData? route = client.GetTopicRouteData(topic);
                if (route is not null) result.Add(route);
            }
            catch (Exception)
            {
                continue;
            }
        }

        return result;
    }

    public TopicRouteData ExamineTopicRoute(string topic)
    {
        TopicRouteData? route = RequireClient().GetTopicRouteData(topic);
        if (route is null) throw new MQClientException("topic " + topic + " not exist");
        return route;
    }

    public TopicConfig ExamineTopicConfig(string addr, string topic)
    {
        PropertyMap ext = new() { ["topic"] = topic, ["lo"] = "true" };
        RemotingCommand response = RequireClient().InvokeSync(
            addr, RequestCode.GetTopicConfig, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            throw new MQBrokerException(ResponseCode.SystemError, "empty topic config for " + topic);
        }

        if (!TopicConfig.Decode(response.Body, out TopicConfig cfg))
        {
            throw new MQBrokerException(ResponseCode.SystemError, "bad topic config body for " + topic);
        }

        return cfg;
    }

    public TopicConfigSerializeWrapper GetAllTopicConfig(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetAllTopicConfig, new PropertyMap(), null, false, to);
        var w = new TopicConfigSerializeWrapper();
        if (response.Body.Length > 0) TopicConfigSerializeWrapper.Decode(response.Body, out w);
        return w;
    }

    public TopicConfigSerializeWrapper GetUserTopicConfig(string brokerAddr,
        bool specialTopic = false, int timeoutMillis = -1)
    {
        TopicConfigSerializeWrapper wrapper = GetAllTopicConfig(brokerAddr, timeoutMillis);
        // 对应 Java getUserTopicConfig：剔除系统 topic 与 %RETRY%/%DLQ%
        TopicList sysTopics = GetSystemTopicListFromBroker(brokerAddr, timeoutMillis);
        var sysSet = new HashSet<string>(sysTopics.Topics);

        var kept = new SortedDictionary<string, TopicConfig>();
        foreach (var kv in wrapper.TopicConfigTable)
        {
            string name = kv.Key;
            if (sysSet.Contains(name)) continue;
            if (MixAll.IsSysTopic(name)) continue;
            bool isRetryOrDlq = name.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal)
                             || name.StartsWith(MixAll.DlqGroupTopicPrefix, StringComparison.Ordinal);
            if (!specialTopic && isRetryOrDlq) continue;
            kept[name] = kv.Value;
        }

        wrapper.TopicConfigTable.Clear();
        foreach (var kv in kept) wrapper.TopicConfigTable[kv.Key] = kv.Value;
        return wrapper;
    }

    public TopicList GetSystemTopicListFromBroker(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetSystemTopicListFromBroker, new PropertyMap(), null, false, to);
        var tl = new TopicList();
        if (response.Body.Length > 0) TopicList.Decode(response.Body, out tl);
        return tl;
    }

    public TopicStatsTable ExamineTopicStats(string topic)
    {
        TopicRouteData route = ExamineTopicRoute(topic);
        var merged = new TopicStatsTable();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            try
            {
                TopicStatsTable part = ExamineTopicStatsByBroker(addr, topic);
                foreach (var kv in part.OffsetTable) merged.OffsetTable[kv.Key] = kv.Value;
                merged.TopicPutTps += part.TopicPutTps;
            }
            catch (Exception e)
            {
                ClientLog.Warn("getTopicStatsInfo error. topic=" + topic + " broker=" + addr + ": " + e.Message);
            }
        }

        if (merged.OffsetTable.Count == 0)
        {
            throw new MQClientException("Not found the topic stats info");
        }

        return merged;
    }

    public TopicStatsTable ExamineTopicStatsByBroker(string brokerAddr, string topic)
    {
        PropertyMap ext = new() { ["topic"] = topic };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetTopicStatsInfo, ext, null, false, _timeoutMillis);
        var t = new TopicStatsTable();
        if (response.Body.Length > 0) TopicStatsTable.Decode(response.Body, out t);
        return t;
    }

    // ---------------------------------------------------------------- 集群 / Broker

    public ClusterInfo FetchBrokerClusterInfo() => RequireClient().GetBrokerClusterInfo();

    public ClusterInfo ExamineBrokerClusterInfo() => FetchBrokerClusterInfo();

    public KvTable FetchBrokerRuntimeStats(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetBrokerRuntimeInfo, new PropertyMap(), null, false, to);
        var t = new KvTable();
        if (response.Body.Length > 0) KvTable.Decode(response.Body, out t);
        return t;
    }

    public KvTable GetBrokerRuntimeInfo(string brokerAddr, int timeoutMillis = -1) =>
        FetchBrokerRuntimeStats(brokerAddr, timeoutMillis);

    /// <summary>
    /// GET_BROKER_CONFIG 的响应体是 **properties 文本**（"k=v\n"），不是 JSON/KvTable。
    /// 早期实现把它当 KvTable JSON 解析，真实 broker 上必然失败。
    /// </summary>
    public PropertyMap GetBrokerConfig(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetBrokerConfig, new PropertyMap(), null, false, to);
        return MixAll.String2Properties(Encoding.UTF8.GetString(response.Body));
    }

    public void UpdateBrokerConfig(string brokerAddr, PropertyMap properties, int timeoutMillis = -1)
    {
        // 对应 Java Validators.checkBrokerConfig 的 brokerPermission 校验
        if (properties.TryGetValue("brokerPermission", out string? permStr))
        {
            int perm = -1;
            if (!int.TryParse(permStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out perm))
            {
                perm = -1;
            }

            if (!PermName.IsValid(perm))
            {
                throw new MQClientException(
                    "brokerPermission value: " + permStr + " is invalid.", ResponseCode.NoPermission);
            }
        }

        string text = MixAll.Properties2String(properties);
        if (text.Length == 0) return;
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RequireClient().InvokeSync(brokerAddr, RequestCode.UpdateBrokerConfig, new PropertyMap(),
            Encoding.UTF8.GetBytes(text), true, to);
    }

    public int WipeWritePermOfBroker(string namesrvAddr, string brokerName)
    {
        PropertyMap ext = new() { ["brokerName"] = brokerName };
        RemotingCommand response = RequireClient().InvokeSync(
            namesrvAddr, RequestCode.WipeWritePermOfBroker, ext, null, false, _timeoutMillis);
        return (int)ExtInt(response, "wipeTopicCount", 0);
    }

    public int AddWritePermOfBroker(string namesrvAddr, string brokerName)
    {
        PropertyMap ext = new() { ["brokerName"] = brokerName };
        RemotingCommand response = RequireClient().InvokeSync(
            namesrvAddr, RequestCode.AddWritePermOfBroker, ext, null, false, _timeoutMillis);
        return (int)ExtInt(response, "addTopicCount", 0);
    }

    public bool CleanUnusedTopic(string clusterName = "", string topic = "")
    {
        MQClientInstance client = RequireClient();
        bool ok = true;
        foreach (string addr in BrokerAddrsOfCluster(client, clusterName))
        {
            try
            {
                client.InvokeSync(addr, RequestCode.CleanUnusedTopic, new PropertyMap(), null, false, _timeoutMillis);
            }
            catch (Exception e)
            {
                ClientLog.Warn("cleanUnusedTopic on " + addr + " failed: " + e.Message);
                ok = false;
            }
        }

        return ok;
    }

    public JsonValue ViewBrokerStatsData(string brokerAddr, string statsName, string statsKey)
    {
        PropertyMap ext = new() { ["statsName"] = statsName, ["statsKey"] = statsKey };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.ViewBrokerStatsData, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0) return JsonValue.Null;
        RemotingSerializable.Decode(response.Body, out JsonValue v);
        return v;
    }

    // ---------------------------------------------------------------- NameServer KV

    /// <summary>Java putKVConfigValue：广播到**每一个** NameServer。</summary>
    public void CreateAndUpdateKvConfig(string ns, string key, string value)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new() { ["namespace"] = ns, ["key"] = key, ["value"] = value };
        string lastError = string.Empty;
        bool anyFailed = false;
        foreach (string addr in client.NameServerAddrs)
        {
            RemotingCommand response = client.InvokeSyncRaw(
                addr, RequestCode.PutKvConfig, ext, null, false, _timeoutMillis);
            if (response.Code != ResponseCode.Success)
            {
                anyFailed = true;
                lastError = response.Remark.Length == 0 ? "put kv config failed" : response.Remark;
            }
        }

        if (anyFailed) throw new MQClientException(lastError);
    }

    public void PutKvConfig(string ns, string key, string value) => CreateAndUpdateKvConfig(ns, key, value);

    /// <summary>返回 false 表示该 key 不存在。</summary>
    public bool GetKvConfig(string ns, string key, out string outValue)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new() { ["namespace"] = ns, ["key"] = key };
        string lastError = string.Empty;
        foreach (string addr in client.NameServerAddrs)
        {
            try
            {
                RemotingCommand response = client.InvokeSyncRaw(
                    addr, RequestCode.GetKvConfig, ext, null, false, _timeoutMillis);
                if (response.Code == ResponseCode.Success)
                {
                    if (response.ExtFields.TryGetValue("value", out string? val))
                    {
                        outValue = val;
                        return true;
                    }

                    outValue = string.Empty;
                    return false;
                }

                // 该 NS 明确返回"没有"，直接返回 false（不再试下一个）
                outValue = string.Empty;
                return false;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("all name servers unreachable: " + lastError);
    }

    public void DeleteKvConfig(string ns, string key)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new() { ["namespace"] = ns, ["key"] = key };
        string lastError = string.Empty;
        bool anyFailed = false;
        foreach (string addr in client.NameServerAddrs)
        {
            RemotingCommand response = client.InvokeSyncRaw(
                addr, RequestCode.DeleteKvConfig, ext, null, false, _timeoutMillis);
            if (response.Code != ResponseCode.Success)
            {
                anyFailed = true;
                lastError = response.Remark.Length == 0 ? "delete kv config failed" : response.Remark;
            }
        }

        if (anyFailed) throw new MQClientException(lastError);
    }

    public KvTable GetKvListByNamespace(string ns)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new() { ["namespace"] = ns };
        string lastError = string.Empty;
        foreach (string addr in client.NameServerAddrs)
        {
            try
            {
                RemotingCommand response = client.InvokeSync(
                    addr, RequestCode.GetKvlistByNamespace, ext, null, false, _timeoutMillis);
                var t = new KvTable();
                if (response.Body.Length > 0) KvTable.Decode(response.Body, out t);
                return t;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("all name servers unreachable: " + lastError);
    }

    // ---------------------------------------------------------------- 订阅组管理

    public void CreateAndUpdateSubscriptionGroupConfig(string addr, SubscriptionGroupConfig config)
    {
        byte[] body = config.Encode();
        RequireClient().InvokeSync(addr, RequestCode.UpdateAndCreateSubscriptiongroup,
            new PropertyMap(), body, true, _timeoutMillis);
    }

    /// <summary>返回 false 表示该订阅组不存在。</summary>
    public bool ExamineSubscriptionGroupConfig(string addr, string group, out SubscriptionGroupConfig outCfg)
    {
        SubscriptionGroupWrapper wrapper = GetAllSubscriptionGroup(addr);
        if (wrapper.SubscriptionGroupTable.TryGetValue(group, out SubscriptionGroupConfig? cfg))
        {
            outCfg = cfg;
            return true;
        }

        outCfg = new SubscriptionGroupConfig();
        return false;
    }

    /// <summary>返回 false 表示响应体为空（配置不存在）。</summary>
    public bool GetSubscriptionGroupConfig(string addr, string group, out SubscriptionGroupConfig outCfg)
    {
        PropertyMap ext = new() { ["group"] = group };
        RemotingCommand response = RequireClient().InvokeSync(
            addr, RequestCode.GetSubscriptiongroupConfig, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            outCfg = new SubscriptionGroupConfig();
            return false;
        }

        SubscriptionGroupConfig.Decode(response.Body, out SubscriptionGroupConfig cfg);
        outCfg = cfg;
        return true;
    }

    /// <summary>
    /// 对应 Java getAllSubscriptionGroup：**分页**累积，直到 groupSeq >= totalGroupNum-1。
    /// 老版本 broker 不带 totalGroupNum，此时一次性返回全部（单轮即结束）。
    /// </summary>
    public SubscriptionGroupWrapper GetAllSubscriptionGroup(string brokerAddr, int timeoutMillis = -1)
    {
        MQClientInstance client = RequireClient();
        int timeout = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        long begin = UtilAll.CurrentTimeMillis();

        JsonValue currentDataVersion = JsonValue.Null;
        long groupSeq = 0;
        bool haveVersion = false;
        var table = new SortedDictionary<string, SubscriptionGroupConfig>();
        JsonValue forbidden = JsonValue.MakeObject();

        while (true)
        {
            long left = timeout - (UtilAll.CurrentTimeMillis() - begin);
            if (left < 0) throw new MQClientException("invokeSync call timeout");

            PropertyMap ext = new()
            {
                ["groupSeq"] = I64Str(groupSeq),
                ["maxGroupNum"] = "10000",
            };
            if (haveVersion) ext["dataVersion"] = DvText(currentDataVersion);

            RemotingCommand response = client.InvokeSyncRaw(
                brokerAddr, RequestCode.GetAllSubscriptiongroupConfig, ext, null, false, (int)left);
            if (response.Code != ResponseCode.Success)
            {
                throw new MQBrokerException(response.Code, response.Remark);
            }

            var wrapper = new SubscriptionGroupWrapper();
            if (response.Body.Length > 0) SubscriptionGroupWrapper.Decode(response.Body, out wrapper);
            foreach (var kv in wrapper.SubscriptionGroupTable) table[kv.Key] = kv.Value;
            if (wrapper.ForbiddenTable.IsObject)
            {
                foreach (var kv in wrapper.ForbiddenTable.ObjectItems())
                {
                    forbidden.Set(kv.Key, kv.Value);
                }
            }

            JsonValue newVersion = wrapper.DataVersion;
            if (!haveVersion)
            {
                currentDataVersion = newVersion;
                haveVersion = true;
            }

            groupSeq += wrapper.SubscriptionGroupTable.Count;

            if (!response.ExtFields.TryGetValue("totalGroupNum", out string? totalStr) ||
                string.IsNullOrEmpty(totalStr))
            {
                // 老 broker：一次返回全部
                break;
            }

            if (!long.TryParse(totalStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out long total))
            {
                break;
            }

            if (DvText(currentDataVersion) != DvText(newVersion))
            {
                ClientLog.Warn("subscription group dataVersion changed, restart paging");
                currentDataVersion = newVersion;
                groupSeq = 0;
                table.Clear();
                forbidden = JsonValue.MakeObject();
                continue;
            }

            if (groupSeq >= total - 1) break;
        }

        var result = new SubscriptionGroupWrapper();
        foreach (var kv in table) result.SubscriptionGroupTable[kv.Key] = kv.Value;
        result.ForbiddenTable = forbidden;
        result.DataVersion = currentDataVersion;
        return result;
    }

    public SubscriptionGroupWrapper GetUserSubscriptionGroup(string brokerAddr, int timeoutMillis = -1)
    {
        SubscriptionGroupWrapper wrapper = GetAllSubscriptionGroup(brokerAddr, timeoutMillis);
        var kept = new SortedDictionary<string, SubscriptionGroupConfig>();
        foreach (var kv in wrapper.SubscriptionGroupTable)
        {
            if (MixAll.IsSysConsumerGroup(kv.Key)) continue;
            if (MixAll.IsPredefinedGroup(kv.Key)) continue;
            kept[kv.Key] = kv.Value;
        }

        wrapper.SubscriptionGroupTable.Clear();
        foreach (var kv in kept) wrapper.SubscriptionGroupTable[kv.Key] = kv.Value;
        return wrapper;
    }

    public void DeleteSubscriptionGroup(string addr, string groupName, bool removeOffset = false)
    {
        PropertyMap ext = new()
        {
            ["groupName"] = groupName,
            ["cleanOffset"] = removeOffset ? "true" : "false",
        };
        RequireClient().InvokeSync(addr, RequestCode.DeleteSubscriptiongroup, ext, null, false, _timeoutMillis);
    }

    // ---------------------------------------------------------------- 消费者 / 生产者连接

    public ConsumerConnection ExamineConsumerConnectionInfo(string consumerGroup, string brokerAddr = "")
    {
        MQClientInstance client = RequireClient();
        string addr = brokerAddr.Length == 0 ? FindFirstBrokerAddr(client) : brokerAddr;
        PropertyMap ext = new() { ["consumerGroup"] = consumerGroup };
        RemotingCommand response = client.InvokeSync(
            addr, RequestCode.GetConsumerConnectionList, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            throw new MQClientException("consumer group " + consumerGroup + " not online");
        }

        ConsumerConnection.Decode(response.Body, out ConsumerConnection cc);
        return cc;
    }

    public ConsumerConnection ExamineConsumerConnection(string consumerGroup, string brokerAddr = "") =>
        ExamineConsumerConnectionInfo(consumerGroup, brokerAddr);

    public ProducerConnection ExamineProducerConnectionInfo(string producerGroup, string brokerAddr = "")
    {
        MQClientInstance client = RequireClient();
        string addr = brokerAddr.Length == 0 ? FindFirstBrokerAddr(client) : brokerAddr;
        PropertyMap ext = new() { ["producerGroup"] = producerGroup };
        RemotingCommand response = client.InvokeSync(
            addr, RequestCode.GetProducerConnectionList, ext, null, false, _timeoutMillis);
        var pc = new ProducerConnection();
        if (response.Body.Length > 0) ProducerConnection.Decode(response.Body, out pc);
        return pc;
    }

    public ConsumerRunningInfo ExamineConsumerRunningInfo(string consumerGroup, string clientId,
        bool jstack = false, string brokerAddr = "")
    {
        MQClientInstance client = RequireClient();
        string addr = brokerAddr.Length == 0 ? FindFirstBrokerAddr(client) : brokerAddr;
        PropertyMap ext = new()
        {
            ["consumerGroup"] = consumerGroup,
            ["clientId"] = clientId,
            ["jstackEnable"] = jstack ? "true" : "false",
        };
        RemotingCommand response = client.InvokeSync(
            addr, RequestCode.GetConsumerRunningInfo, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            throw new MQClientException("no running info for client " + clientId);
        }

        ConsumerRunningInfo.Decode(response.Body, out ConsumerRunningInfo ri);
        return ri;
    }

    public GetConsumerListByGroupResponseBody GetConsumerListByGroup(string consumerGroup,
        string brokerAddr = "")
    {
        MQClientInstance client = RequireClient();
        string addr = brokerAddr.Length == 0 ? FindFirstBrokerAddr(client) : brokerAddr;
        return client.GetConsumerListByGroup(consumerGroup, addr);
    }

    // ---------------------------------------------------------------- 消费统计

    public ConsumeStats ExamineConsumeStats(string brokerAddr, string consumerGroup,
        string topic = "", List<string>? topicList = null)
    {
        PropertyMap ext = new() { ["consumerGroup"] = consumerGroup };
        if (topic.Length > 0) ext["topic"] = topic;
        if (topicList is { Count: > 0 })
        {
            var joined = new StringBuilder();
            for (int i = 0; i < topicList.Count; ++i)
            {
                if (i > 0) joined.Append(';');
                joined.Append(topicList[i]);
            }

            ext["topicList"] = joined.ToString();
        }

        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetConsumeStats, ext, null, false, _timeoutMillis);
        var cs = new ConsumeStats();
        if (response.Body.Length > 0) ConsumeStats.Decode(response.Body, out cs);
        return cs;
    }

    public ConsumeStatsList FetchConsumeStatsInBroker(string brokerAddr, bool isOrder = false,
        int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        PropertyMap ext = new() { ["isOrder"] = isOrder ? "true" : "false" };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.GetBrokerConsumeStats, ext, null, false, to);
        var sl = new ConsumeStatsList();
        if (response.Body.Length > 0) ConsumeStatsList.Decode(response.Body, out sl);
        return sl;
    }

    public HashSet<string> QueryTopicConsumeByWho(string brokerAddr, string topic)
    {
        PropertyMap ext = new() { ["topic"] = topic };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.QueryTopicConsumeByWho, ext, null, false, _timeoutMillis);
        var groups = new HashSet<string>();
        if (response.Body.Length == 0) return groups;
        if (!RemotingSerializable.Decode(response.Body, out JsonValue v)) return groups;
        JsonValue? arr = v.Find("groupList");
        if (arr is { IsArray: true })
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsString) groups.Add(arr.At(i).StringValue());
            }
        }

        return groups;
    }

    /// <summary>
    /// 对应 Java MQClientAPIImpl#queryTopicsByConsumer:2525（343）的单 broker 原始调用。
    /// broker 端走 AdminBrokerProcessor#queryTopicsByConsumer:2421 →
    /// ConsumerOffsetManager#whichTopicByConsumer：**从位点表**（topic@group 键）反查该组
    /// 消费过哪些 topic，所以组从没提交过位点时回空表，这是预期而不是 bug。
    /// </summary>
    public TopicList QueryTopicsByConsumerToBroker(string brokerAddr, string group)
    {
        PropertyMap ext = new() { ["group"] = group };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.QueryTopicsByConsumer, ext, null, false, _timeoutMillis);
        var tl = new TopicList();
        if (response.Body.Length > 0) TopicList.Decode(response.Body, out tl);
        return tl;
    }

    /// <summary>
    /// 对应 Java DefaultMQAdminExt#queryTopicsByConsumer（DefaultMQAdminExtImpl:1078）：
    /// 先按 %RETRY%&lt;group&gt; 查路由，再对路由里每个 broker 下发 343 并合并
    /// （Java 的 TopicList.topicList 是 Set&lt;string&gt;，所以这里同样去重）。
    /// </summary>
    public TopicList QueryTopicsByConsumer(string group)
    {
        TopicRouteData route = ExamineTopicRoute(MixAll.GetRetryTopic(group));
        TopicList merged = new();
        HashSet<string> seen = new();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            TopicList part = QueryTopicsByConsumerToBroker(addr, group);
            foreach (string topic in part.Topics)
            {
                if (seen.Add(topic)) merged.Topics.Add(topic);
            }
        }
        return merged;
    }

    public JsonValue QuerySubscription(string brokerAddr, string group, string topic)
    {
        PropertyMap ext = new() { ["group"] = group, ["topic"] = topic };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.QuerySubscriptionByConsumer, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0) return JsonValue.Null;
        RemotingSerializable.Decode(response.Body, out JsonValue v);
        return v;
    }

    public JsonValue GetConsumeStatus(string brokerAddr, string topic, string group, string clientAddr = "")
    {
        PropertyMap ext = new()
        {
            ["topic"] = topic,
            ["group"] = group,
            ["clientAddr"] = clientAddr,
        };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.InvokeBrokerToGetConsumerStatus, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0) return JsonValue.Null;
        if (!RemotingSerializable.Decode(response.Body, out JsonValue v)) return JsonValue.Null;
        return v.Get("consumerTable");
    }

    public void CloneGroupOffset(string brokerAddr, string srcGroup, string destGroup,
        string topic, bool offline = false)
    {
        PropertyMap ext = new()
        {
            ["srcGroup"] = srcGroup,
            ["destGroup"] = destGroup,
            ["topic"] = topic,
            ["offline"] = offline ? "true" : "false",
        };
        RequireClient().InvokeSync(brokerAddr, RequestCode.CloneGroupOffset, ext, null, false, _timeoutMillis);
    }

    // ---------------------------------------------------------------- Offset 管理

    public long MaxOffset(MessageQueue mq) => RequireClient().GetMaxOffset(mq);

    public long MinOffset(MessageQueue mq) => RequireClient().GetMinOffset(mq);

    /// <summary>对应 Java MQAdminImpl:189：显式下发 LOWER 边界。</summary>
    public long SearchOffset(MessageQueue mq, long timestamp) =>
        SearchLowerBoundaryOffset(mq, timestamp);

    /// <summary>对应 Java DefaultMQAdminExt:133。</summary>
    public long SearchLowerBoundaryOffset(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByBoundary(mq, timestamp, BoundaryType.Lower);

    /// <summary>
    /// 对应 Java DefaultMQAdminExt:137。时间戳落在队尾之后时 UPPER 给最后一条自身的位点，
    /// LOWER 给它的下一个位点（maxOffset）。
    /// </summary>
    public long SearchUpperBoundaryOffset(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByBoundary(mq, timestamp, BoundaryType.Upper);

    public long EarliestMsgStoreTime(MessageQueue mq)
    {
        MQClientInstance client = RequireClient();
        string addr = BrokerAddrForMq(client, mq);
        PropertyMap ext = new()
        {
            ["topic"] = mq.Topic,
            ["queueId"] = I64Str(mq.QueueId),
            ["brokerName"] = mq.BrokerName,
        };
        RemotingCommand response = client.InvokeSync(
            addr, RequestCode.GetEarliestMsgStoretime, ext, null, false, _timeoutMillis);
        return ExtInt(response, "timestamp", 0);
    }

    /// <summary>返回 false 表示该消费组在该队列上尚无位点（broker 回 QUERY_NOT_FOUND）。</summary>
    public bool ExamineConsumerOffset(string consumerGroup, MessageQueue mq, out long outOffset) =>
        RequireClient().QueryConsumerOffset(consumerGroup, mq, out outOffset);

    public void UpdateConsumerOffset(string consumerGroup, MessageQueue mq, long offset) =>
        RequireClient().UpdateConsumerOffset(consumerGroup, mq, offset);

    public void UpdateConsumerOffsetToBroker(string brokerAddr, string consumerGroup,
        MessageQueue mq, long offset) =>
        RequireClient().UpdateConsumerOffset(consumerGroup, mq, offset, 5000, brokerAddr);

    /// <summary>
    /// 对应 Java resetOffsetByTimestamp：逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET
    /// （broker 端按 timestamp 计算新位点，并同步在线消费者 + 更新 offset 表）。
    ///
    /// 注意：这里**不再**走"逐队列 searchOffset + updateConsumerOffset"的旧本地实现——
    /// 那不会同步在线消费者，也不会做 broker 端一致性校验。
    ///
    /// language 必须覆盖为 CPP：broker 端只对 CPP/PYTHON（这里用 CPP）才回可解析的 offsetTable
    /// 响应体；否则返回空体，调用方会拿到空 map 并抛错。
    /// </summary>
    public SortedDictionary<MessageQueue, long> ResetOffsetByTimestamp(string topic, string group,
        long timestamp, bool isForce = true, string clusterName = "", bool isCpp = true)
    {
        string routeTopic = topic;
        string wheelTimer = MixAll.SystemTopicPrefix + "wheel_timer";
        if (!string.IsNullOrEmpty(topic) &&
            (MixAll.IsLmq(topic) || topic == wheelTimer) && !string.IsNullOrEmpty(clusterName))
        {
            routeTopic = clusterName;
        }

        TopicRouteData route = ExamineTopicRoute(routeTopic);

        var allOffsets = new SortedDictionary<MessageQueue, long>();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            // queueId/offset 都传 -1：Java 的"按 timestamp 重置整个 topic"那个重载
            foreach (var kv in InvokeBrokerToResetOffset(addr, topic, group, timestamp, isForce,
                         isCpp, -1, -1))
            {
                allOffsets[kv.Key] = kv.Value;
            }
        }

        if (allOffsets.Count == 0)
        {
            throw new MQClientException("reset offset failed, no broker returned offset table");
        }

        return allOffsets;
    }

    /// <summary>
    /// 一笔 INVOKE_BROKER_TO_RESET_OFFSET(222)，回 broker 实际重置的队列表。
    ///
    /// Java 在这里有**两个**重载：不带 queueId 的按 timestamp 重置整个 topic，带
    /// queueId + offset 的只重置单个队列（MQClientAPIImpl#invokeBrokerToResetOffset）。
    /// 这里用哨兵值表达"字段为空"：queueId &lt; 0 即 Java 的默认 -1，offset &lt; 0 即
    /// Java 的"offset=-1 表示 offset 为 null"。
    /// </summary>
    private SortedDictionary<MessageQueue, long> InvokeBrokerToResetOffset(string brokerAddr,
        string topic, string group, long timestamp, bool isForce, bool isCpp, int queueId,
        long offset)
    {
        MQClientInstance client = RequireClient();
        PropertyMap ext = new()
        {
            ["topic"] = topic,
            ["group"] = group,
            ["timestamp"] = I64Str(timestamp),
            ["force"] = isForce ? "true" : "false",
            // Java：offset=-1 表示 offset 为空
            ["offset"] = I64Str(offset),
        };
        if (queueId >= 0) ext["queueId"] = I64Str(queueId);
        RemotingCommand response = client.InvokeSyncRaw(
            brokerAddr, RequestCode.InvokeBrokerToResetOffset, ext, null, false, _timeoutMillis,
            isCpp ? LanguageCode.Cpp : -1);
        if (response.Code != ResponseCode.Success)
        {
            throw new MQClientException(
                response.Remark.Length == 0 ? "reset offset failed" : response.Remark, response.Code);
        }

        var offsets = new SortedDictionary<MessageQueue, long>();
        if (response.Body.Length > 0 &&
            ResetOffsetBody.Decode(response.Body, out ResetOffsetBody body))
        {
            foreach (var kv in body.OffsetTable) offsets[kv.Key] = kv.Value;
        }

        return offsets;
    }

    /// <summary>
    /// 对应 Java DefaultMQAdminExt#resetOffsetByQueueId（DefaultMQAdminExtImpl:1827）：
    /// **两笔** RPC，缺一不可。
    /// 1. UpdateConsumerOffset(25) 直接把 offsetTable 改成目标位点；
    /// 2. 带 queueId + offset 的 222 走 AdminBrokerProcessor#resetOffsetInner，先按
    ///    [min, max+1] 校验目标位点（越界回 SYSTEM_ERROR
    ///    "Target offset N not in consume queue range [min-max]"），再
    ///    ConsumerOffsetManager#assignResetOffset——它同时写 resetOffsetTable（一次性，
    ///    下次 pull 用 queryThenEraseResetOffset 取走）和 offsetTable，并清掉该队列的 POP
    ///    在途计数。只做第 1 步的话在线消费者仍按自己内存里的位点继续拉。
    ///
    /// Java 返回 void（只打日志），这里把 broker 报回的队列表返回，便于调用方核对。
    ///
    /// 注意实测（5.5.1 真机）这两笔 RPC **不是原子的**：ConsumerOffsetManager#commitOffset
    /// 只做覆盖写（offset 变小也只打 [NOTIFYME] warn，不做区间校验），所以第 2 笔被
    /// resetOffsetInner 以 "Target offset N not in consume queue range" 拒绝时，第 1 笔
    /// 已经把非法位点落库。Java 同样如此，这里不做保护性回滚。
    /// </summary>
    public SortedDictionary<MessageQueue, long> ResetOffsetByQueueId(string brokerAddr,
        string consumerGroup, string topic, int queueId, long resetOffset)
    {
        UpdateConsumerOffsetToBroker(brokerAddr, consumerGroup,
            new MessageQueue(topic, string.Empty, queueId), resetOffset);
        // Java 的单队列重载不传 force（默认 false）、timestamp 传 0（位点已给定，不参与算）
        return InvokeBrokerToResetOffset(brokerAddr, topic, consumerGroup, 0, false, false,
            queueId, resetOffset);
    }

    /// <summary>
    /// 对应 Java resetOffsetNew：先试新版（broker 端重置），消费者不在线再退化到旧版。
    /// </summary>
    public void ResetOffsetNew(string consumerGroup, string topic, long timestamp)
    {
        try
        {
            ResetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
        }
        catch (MQClientException e)
        {
            if (e.ResponseCode == ResponseCode.ConsumerNotOnline)
            {
                ResetOffsetByTimestampOld(consumerGroup, topic, timestamp, true);
                return;
            }

            throw;
        }
    }

    /// <summary>
    /// 对应 Java resetOffsetByTimestampOld：逐队列 searchOffset 后按 force 决策写回。
    /// </summary>
    public SortedDictionary<MessageQueue, long> ResetOffsetByTimestampOld(string consumerGroup,
        string topic, long timestamp, bool force = true)
    {
        MQClientInstance client = RequireClient();
        TopicRouteData route = ExamineTopicRoute(topic);
        var result = new SortedDictionary<MessageQueue, long>();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            foreach (QueueData qd in route.QueueDatas)
            {
                if (qd.BrokerName != bd.BrokerName) continue;
                for (int queueId = 0; queueId < qd.ReadQueueNums; ++queueId)
                {
                    var mq = new MessageQueue(topic, bd.BrokerName, queueId);
                    long consumerOffset = 0;
                    try
                    {
                        client.QueryConsumerOffset(consumerGroup, mq, out consumerOffset, 5000, addr);
                    }
                    catch (Exception)
                    {
                        consumerOffset = 0;
                    }

                    long resetOffset;
                    if (timestamp == -1)
                    {
                        resetOffset = client.GetMaxOffset(mq, 5000, addr);
                    }
                    else
                    {
                        resetOffset = client.SearchOffsetByTimestamp(mq, timestamp, 5000, addr);
                    }

                    if (force || resetOffset <= consumerOffset)
                    {
                        client.UpdateConsumerOffset(consumerGroup, mq, resetOffset, 5000, addr);
                        result[mq] = resetOffset;
                    }
                }
            }
        }

        return result;
    }

    // ---------------------------------------------------------------- 消息查询

    public List<MessageExt> QueryMessage(string topic, string key, int maxNum, long begin, long end) =>
        RequireClient().QueryMessageAllBrokers(
            topic, key, maxNum, begin, end, MessageConst.IndexKeyType, false);

    /// <summary>
    /// 返回 false 表示未命中（注意：默认文件索引下 uniqKey 查询需要 RocksDB 索引）。
    /// </summary>
    public bool QueryMessageByUniqKey(string topic, string uniqKey, out MessageExt outMsg)
    {
        // 对应 Java queryMessageByUniqKey：indexType="U" + extFields["_UNIQUE_KEY_QUERY"]="true"。
        // 注意：broker 侧 uniqKey 倒排索引只有 RocksDB 索引实现（IndexRocksDBStore）支持；
        // 默认文件索引下该查询会返回空，属 broker 配置差异而非客户端问题。
        long now = UtilAll.CurrentTimeMillis();
        List<MessageExt> msgs = RequireClient().QueryMessageAllBrokers(
            topic, uniqKey, 32, 0, now + 3600L * 1000, MessageConst.IndexUniqueType, true);
        if (msgs.Count == 0)
        {
            outMsg = new MessageExt();
            return false;
        }

        outMsg = msgs[0];
        return true;
    }

    public List<MessageExt> QueryMessageByKey(string topic, string key, int maxNum = 32)
    {
        long now = UtilAll.CurrentTimeMillis();
        return RequireClient().QueryMessageAllBrokers(
            topic, key, maxNum, 0, now + 3600L * 1000, MessageConst.IndexKeyType, false);
    }

    /// <summary>
    /// 对应 Java DefaultMQAdminExtImpl.viewMessage：先按 offset msgId 直查 commitLog，
    /// 解不出或查不到就退回 uniqKey（5.x 客户端返回的 msgId 本身就是 uniqKey，
    /// 硬解只会得到越界的垃圾端口）。
    /// </summary>
    public MessageExt ViewMessage(string topic, string msgId)
    {
        Exception? byIdError;
        try
        {
            if (!MessageDecoder.DecodeMessageId(msgId, out string ip, out int port, out long offset)
                || port <= 0 || port > ushort.MaxValue)
            {
                throw new MQClientException("not a valid offset msgId: " + msgId,
                    ResponseCode.NoMessage);
            }

            PropertyMap ext = new()
            {
                ["topic"] = topic,
                ["offset"] = I64Str(offset),
            };
            RemotingCommand response = RequireClient().InvokeSync(
                ip + ":" + port.ToString(CultureInfo.InvariantCulture),
                RequestCode.ViewMessageById, ext, null, false, _timeoutMillis);
            if (response.Body.Length == 0)
            {
                throw new MQBrokerException(ResponseCode.NoMessage, "message not found: " + msgId);
            }

            List<MessageExt> msgs = MessageDecoder.DecodeMessages(response.Body, true);
            if (msgs.Count == 0)
            {
                throw new MQBrokerException(ResponseCode.NoMessage, "message not found: " + msgId);
            }

            return msgs[0];
        }
        catch (Exception e)
        {
            // Java 同样只 warn，然后走 uniqKey 兜底
            byIdError = e;
        }

        if (QueryMessageByUniqKey(topic, msgId, out MessageExt found))
        {
            return found;
        }

        throw new MQClientException(
            "viewMessage failed: neither offset msgId nor uniq key matched message " + msgId
            + " of " + topic, byIdError, ResponseCode.NoMessage);
    }

    public QueryConsumeQueueResponseBody QueryConsumeQueue(string brokerAddr, string topic,
        int queueId, long index, int count = 32, string consumerGroup = "")
    {
        PropertyMap ext = new()
        {
            ["topic"] = topic,
            ["queueId"] = I64Str(queueId),
            ["index"] = I64Str(index),
            ["count"] = I64Str(count),
            ["consumerGroup"] = consumerGroup,
        };
        RemotingCommand response = RequireClient().InvokeSync(
            brokerAddr, RequestCode.QueryConsumeQueue, ext, null, false, _timeoutMillis);
        var body = new QueryConsumeQueueResponseBody();
        if (response.Body.Length > 0) QueryConsumeQueueResponseBody.Decode(response.Body, out body);
        return body;
    }
}
