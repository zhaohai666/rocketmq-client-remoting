// Coverage: topic CRUD/config, broker cluster info and runtime info/config, NameServer KV
// config, subscription group management, consumer/producer connections, consume stats,
// offset management (including real broker offset reset), and message queries
// (key / uniqKey / msgId / ConsumeQueue).
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public enum TrackType
{
    Consumed,
    ConsumedButFiltered,
    Pull,
    NotConsumeYet,
    NotOnline,
    ConsumeBroadcasting,
    Unknown,
}

public sealed class MessageTrack
{
    public string ConsumerGroup { get; set; } = string.Empty;
    public TrackType TrackType { get; set; } = TrackType.Unknown;
    public string? ExceptionDesc { get; set; }

    public override string ToString()
        => "MessageTrack [consumerGroup=" + ConsumerGroup + ", trackType=" + TrackType
           + ", exceptionDesc=" + ExceptionDesc + "]";
}

public sealed class GroupForbidden
{
    public string Topic { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;
    public bool Readable { get; set; }

    /// <summary>Returns false when decoding fails (does not throw).</summary>
    public static bool Decode(byte[] data, out GroupForbidden outValue)
    {
        outValue = new GroupForbidden();
        if (!RemotingSerializable.Decode(data, out JsonValue v) || !v.IsObject) return false;
        if (v.TryGetString("topic", out string topic)) outValue.Topic = topic;
        if (v.TryGetString("group", out string group)) outValue.Group = group;
        if (v.TryGetBool("readable", out bool readable)) outValue.Readable = readable;
        return true;
    }
}

public sealed class DefaultMQAdminExt
{
    // Corresponds to DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3
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

    // ---------------------------------------------------------------- Config and lifecycle

    public void SetNamesrvAddr(string addr)
    {
        _nameServerAddrs.Clear();
        int start = 0;
        while (start <= addr.Length)
        {
            int semi = addr.IndexOf(';', start);
            string piece = semi < 0 ? addr[start..] : addr[start..semi];
            // Trim leading/trailing whitespace (Trim covers spaces, tabs, etc. — strict)
            piece = piece.Trim();
            if (piece.Length > 0) _nameServerAddrs.Add(piece);
            if (semi < 0) break;
            start = semi + 1;
        }
    }

    public void SetNameServerAddresses(IReadOnlyList<string> addrs) =>
        _nameServerAddrs = new List<string>(addrs);

    /// <summary>Change instanceName at runtime (does not rebuild the client).</summary>
    public void SetInstanceName(string name) => _instanceName = name;

    /// <summary>instanceName is rewritten in place by Start(), so this reads the actual post-start value.</summary>
    public string GetInstanceName() => _instanceName;

    /// <summary>Only available after Start() (corresponds to the other facades' ClientId).</summary>
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

    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>When true, every request carries <c>ReqT=0</c> and the clientId gets an extra <c>@STREAM</c> suffix.</summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    public string NamespaceV2
    {
        get => _namespaceV2;
        set => _namespaceV2 = value ?? string.Empty;
    }

    private string _namespaceV2 = string.Empty;

    public bool VipChannelEnabled
    {
        get => _vipChannelEnabled;
        set => _vipChannelEnabled = value;
    }

    public int PollNameServerIntervalMillis
    {
        get => _pollNameServerIntervalMillis;
        set => _pollNameServerIntervalMillis = value;
    }

    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>Convenience entry: builds an AclClientRPCHook from accessKey/secretKey (optional securityToken).</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ACL hook, bound to the transport layer of the MQClientInstance on Start()
    private IRpcHook? _rpcHook;
    private string _unitName = string.Empty;
    private bool _enableStreamRequestType;
    private bool _vipChannelEnabled;
    private int _pollNameServerIntervalMillis = 30000;

    public void SetTimeoutMillis(int millis) => _timeoutMillis = millis;

    public int GetTimeoutMillis() => _timeoutMillis;

    public void AddKvNamespaceToDeleteList(string ns) => _kvNamespaceToDeleteList.Add(ns);

    public void Start()
    {
        if (_started) return;
        if (_nameServerAddrs.Count == 0)
        {
            throw new MQClientException("name server address is not set");
        }

        _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
        if (string.IsNullOrEmpty(_clientId))
        {
            _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
        }

        IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook, () => _namespaceV2);
        _mqClient = MQClientManager.Instance.GetOrCreateMQClientInstance(_clientId,
            () => new MQClientInstance(_clientId, _nameServerAddrs, unitName: _unitName,
                pollNameServerIntervalMillis: _pollNameServerIntervalMillis));
        if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
        {
            ClientLog.Warn("admin rpc hook ignored: MQClientInstance already has one (clientId="
                + _clientId + ")");
        }
        try
        {
            _mqClient.Start();
        }
        catch
        {
            // If startup fails, hand the reference back: otherwise the refcount
            // misses a decrement and the shared instance never reaches zero.
            MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
            _mqClient = null;
            throw;
        }

        _started = true;
    }

    public void Shutdown()
    {
        if (!_started) return;
        _started = false;
        if (_mqClient is not null)
        {
            MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
            _mqClient = null;
        }
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

    /// <summary>When the VIP channel switch (VipChannelEnabled) is on, rewrites broker addresses to the VIP port.
    /// NameServer calls do not go through here — VIP ports only exist on the broker side.</summary>
    private string AddrForRpc(string addr) => MixAll.BrokerVipChannel(_vipChannelEnabled, addr);

    public MQClientInstance GetMQClientInstance() => RequireClient();

    /// <summary>Gets the underlying client instance (alias of GetMQClientInstance).</summary>
    public MQClientInstance Client() => GetMQClientInstance();

    // ---------------------------------------------------------------- Internal utilities

    private static string FirstBrokerAddr(MQClientInstance client)
    {
        ClusterInfo ci = client.GetBrokerClusterInfo();
        List<string> addrs = ci.GetBrokerAddrs();
        if (addrs.Count == 0) throw new MQClientException("no broker address available");
        return addrs[0];
    }

    private static string FindFirstBrokerAddr(MQClientInstance client) => FirstBrokerAddr(client);

    private static string PublishAddrForMq(MQClientInstance client, MessageQueue mq) =>
        client.PublishAddrFor(mq.BrokerName, mq.Topic);

    private static List<string> BrokerAddrsOfCluster(MQClientInstance client, string clusterName)
    {
        ClusterInfo ci = client.GetBrokerClusterInfo();
        if (clusterName.Length == 0) return ci.GetBrokerAddrs();
        List<string> addrs = ci.GetBrokerAddrsOfCluster(clusterName);
        if (addrs.Count == 0) return ci.GetBrokerAddrs();
        return addrs;
    }

    private static string I64Str(long v) => v.ToString(CultureInfo.InvariantCulture);

    /// <summary>Reads an integer from the response extFields (missing/empty/invalid returns fallback).</summary>
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

    /// <summary>Equivalent text of a JsonValue (used to compare whether DataVersion changed).</summary>
    private static string DvText(JsonValue v) => v.IsNull ? "null" : v.Dump();

    // ---------------------------------------------------------------- Topic management

    // Corresponds to DefaultMQAdminExt.createTopic: key / topicSysFlag are ignored parameters;
    // it goes straight to createTopicInRoute (default TopicConfig defaultTopic=TBW102, perm=READ|WRITE).
    public void CreateTopic(string key, string newTopic, int queueNum = 4, int topicSysFlag = 0)
    {
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
                RemotingCommand response = client.InvokeSync(ns, RequestCode.GetTopicsByCluster, ext, null, false, _timeoutMillis);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(addr), RequestCode.GetTopicConfig, ext, null, false, _timeoutMillis);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetAllTopicConfig, new PropertyMap(), null, false, to);
        var w = new TopicConfigSerializeWrapper();
        if (response.Body.Length > 0) TopicConfigSerializeWrapper.Decode(response.Body, out w);
        return w;
    }

    public TopicConfigSerializeWrapper GetUserTopicConfig(string brokerAddr,
        bool specialTopic = false, int timeoutMillis = -1)
    {
        TopicConfigSerializeWrapper wrapper = GetAllTopicConfig(brokerAddr, timeoutMillis);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetSystemTopicListFromBroker, new PropertyMap(), null, false, to);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetTopicStatsInfo, ext, null, false, _timeoutMillis);
        var t = new TopicStatsTable();
        if (response.Body.Length > 0) TopicStatsTable.Decode(response.Body, out t);
        return t;
    }

    // ---------------------------------------------------------------- Cluster / Broker

    public ClusterInfo FetchBrokerClusterInfo() => RequireClient().GetBrokerClusterInfo();

    public ClusterInfo ExamineBrokerClusterInfo() => FetchBrokerClusterInfo();

    public KvTable FetchBrokerRuntimeStats(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetBrokerRuntimeInfo, new PropertyMap(), null, false, to);
        var t = new KvTable();
        if (response.Body.Length > 0) KvTable.Decode(response.Body, out t);
        return t;
    }

    public KvTable GetBrokerRuntimeInfo(string brokerAddr, int timeoutMillis = -1) =>
        FetchBrokerRuntimeStats(brokerAddr, timeoutMillis);

    /// <summary>
    /// The GET_BROKER_CONFIG response body is a **properties text** ("k=v\n"), not JSON/KvTable.
    /// The early implementation parsed it as KvTable JSON, which necessarily failed against a
    /// real broker.
    /// </summary>
    public PropertyMap GetBrokerConfig(string brokerAddr, int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetBrokerConfig, new PropertyMap(), null, false, to);
        return MixAll.String2Properties(Encoding.UTF8.GetString(response.Body));
    }

    public void UpdateBrokerConfig(string brokerAddr, PropertyMap properties, int timeoutMillis = -1)
    {
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
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.UpdateBrokerConfig, new PropertyMap(),
            Encoding.UTF8.GetBytes(text), true, to);
    }

    public int WipeWritePermOfBroker(string namesrvAddr, string brokerName)
    {
        PropertyMap ext = new() { ["brokerName"] = brokerName };
        RemotingCommand response = RequireClient().InvokeSync(namesrvAddr, RequestCode.WipeWritePermOfBroker, ext, null, false, _timeoutMillis);
        return (int)ExtInt(response, "wipeTopicCount", 0);
    }

    public int AddWritePermOfBroker(string namesrvAddr, string brokerName)
    {
        PropertyMap ext = new() { ["brokerName"] = brokerName };
        RemotingCommand response = RequireClient().InvokeSync(namesrvAddr, RequestCode.AddWritePermOfBroker, ext, null, false, _timeoutMillis);
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
                client.InvokeSync(AddrForRpc(addr), RequestCode.CleanUnusedTopic, new PropertyMap(), null, false, _timeoutMillis);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.ViewBrokerStatsData, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0) return JsonValue.Null;
        RemotingSerializable.Decode(response.Body, out JsonValue v);
        return v;
    }

    // ---------------------------------------------------------------- NameServer KV

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

    /// <summary>Returns false when the key does not exist.</summary>
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

                // This NS explicitly said "not found": return false directly (don't try the next one)
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
                RemotingCommand response = client.InvokeSync(addr, RequestCode.GetKvlistByNamespace, ext, null, false, _timeoutMillis);
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

    // ---------------------------------------------------------------- Subscription group management

    public void CreateAndUpdateSubscriptionGroupConfig(string addr, SubscriptionGroupConfig config)
    {
        byte[] body = config.Encode();
        RequireClient().InvokeSync(AddrForRpc(addr), RequestCode.UpdateAndCreateSubscriptiongroup,
            new PropertyMap(), body, true, _timeoutMillis);
    }

    /// <summary>Returns false when the subscription group does not exist.</summary>
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

    /// <summary>Returns false when the response body is empty (the config does not exist).</summary>
    public bool GetSubscriptionGroupConfig(string addr, string group, out SubscriptionGroupConfig outCfg)
    {
        PropertyMap ext = new() { ["group"] = group };
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(addr), RequestCode.GetSubscriptiongroupConfig, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            outCfg = new SubscriptionGroupConfig();
            return false;
        }

        SubscriptionGroupConfig.Decode(response.Body, out SubscriptionGroupConfig cfg);
        outCfg = cfg;
        return true;
    }

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
                // Old broker: returns everything in one shot
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
        RequireClient().InvokeSync(AddrForRpc(addr), RequestCode.DeleteSubscriptiongroup, ext, null, false, _timeoutMillis);
    }

    // ---------------------------------------------------------------- Consumer / producer connections

    public ConsumerConnection ExamineConsumerConnectionInfo(string consumerGroup, string brokerAddr = "")
    {
        MQClientInstance client = RequireClient();
        string addr = brokerAddr.Length == 0 ? FindFirstBrokerAddr(client) : brokerAddr;
        PropertyMap ext = new() { ["consumerGroup"] = consumerGroup };
        RemotingCommand response = client.InvokeSync(AddrForRpc(addr), RequestCode.GetConsumerConnectionList, ext, null, false, _timeoutMillis);
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
        RemotingCommand response = client.InvokeSync(AddrForRpc(addr), RequestCode.GetProducerConnectionList, ext, null, false, _timeoutMillis);
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
        RemotingCommand response = client.InvokeSync(AddrForRpc(addr), RequestCode.GetConsumerRunningInfo, ext, null, false, _timeoutMillis);
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

    // ---------------------------------------------------------------- Consumption statistics

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

        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetConsumeStats, ext, null, false, _timeoutMillis);
        var cs = new ConsumeStats();
        if (response.Body.Length > 0) ConsumeStats.Decode(response.Body, out cs);
        return cs;
    }

    public ConsumeStatsList FetchConsumeStatsInBroker(string brokerAddr, bool isOrder = false,
        int timeoutMillis = -1)
    {
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        PropertyMap ext = new() { ["isOrder"] = isOrder ? "true" : "false" };
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.GetBrokerConsumeStats, ext, null, false, to);
        var sl = new ConsumeStatsList();
        if (response.Body.Length > 0) ConsumeStatsList.Decode(response.Body, out sl);
        return sl;
    }

    public HashSet<string> QueryTopicConsumeByWho(string brokerAddr, string topic)
    {
        PropertyMap ext = new() { ["topic"] = topic };
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.QueryTopicConsumeByWho, ext, null, false, _timeoutMillis);
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


    public ConsumeStats ExamineConsumeStatsGroup(string consumerGroup, string topic = "")
    {
        TopicRouteData route = ExamineTopicRoute(MixAll.GetRetryTopic(consumerGroup));
        var merged = new ConsumeStats();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            ConsumeStats part = ExamineConsumeStats(addr, consumerGroup, topic);
            foreach (var kv in part.OffsetTable) merged.OffsetTable[kv.Key] = kv.Value;
            merged.ConsumeTps += part.ConsumeTps;
        }

        if (merged.OffsetTable.Count == 0)
        {
            throw new MQClientException("no consume stats for group " + consumerGroup,
                ResponseCode.SystemError);
        }

        return merged;
    }

    public bool Consumed(MessageExt msg, string group)
    {
        ConsumeStats cstats = ExamineConsumeStatsGroup(group);
        ClusterInfo ci = ExamineBrokerClusterInfo();
        string? storeHost = msg.StoreHostString;
        foreach (var kv in cstats.OffsetTable)
        {
            MessageQueue mq = kv.Key;
            if (mq.Topic == msg.Topic && mq.QueueId == msg.QueueId
                && ci.BrokerAddrTable.TryGetValue(mq.BrokerName, out BrokerData? bd))
            {
                if (bd.BrokerAddrs.TryGetValue(MixAll.MasterId, out string? addr))
                {
                    if (addr == storeHost && kv.Value.ConsumerOffset > msg.QueueOffset)
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public List<MessageTrack> MessageTrackDetail(MessageExt msg)
    {
        var result = new List<MessageTrack>();
        TopicRouteData route = ExamineTopicRoute(msg.Topic);
        string? brokerAddr = null;
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length != 0)
            {
                brokerAddr = addr;
                break;
            }
        }

        if (brokerAddr is null) return result;

        HashSet<string> groups = QueryTopicConsumeByWho(brokerAddr, msg.Topic);
        foreach (string group in groups.OrderBy(g => g, StringComparer.Ordinal))
        {
            var mt = new MessageTrack { ConsumerGroup = group };
            ConsumerConnection cc;
            try
            {
                cc = ExamineConsumerConnectionInfo(group);
            }
            catch (MQBrokerException e)
            {
                if (e.ResponseCode == ResponseCode.ConsumerNotOnline)
                {
                    mt.TrackType = TrackType.NotOnline;
                }

                mt.ExceptionDesc = "CODE:" + e.ResponseCode + " DESC:" + e.Message;
                result.Add(mt);
                continue;
            }
            catch (Exception e)
            {
                mt.ExceptionDesc = e.Message;
                result.Add(mt);
                continue;
            }

            switch (cc.ConsumeType)
            {
                case "CONSUME_ACTIVELY":
                    mt.TrackType = TrackType.Pull;
                    break;
                case "CONSUME_PASSIVELY":
                    bool ifConsumed;
                    try
                    {
                        ifConsumed = Consumed(msg, group);
                    }
                    catch (MQClientException e)
                    {
                        if (e.ResponseCode == ResponseCode.ConsumerNotOnline)
                        {
                            mt.TrackType = TrackType.NotOnline;
                            mt.ExceptionDesc = "CODE:" + e.ResponseCode + " DESC:" + e.Message;
                        }
                        else if (e.ResponseCode == ResponseCode.BroadcastConsumption)
                        {
                            mt.TrackType = TrackType.ConsumeBroadcasting;
                        }

                        result.Add(mt);
                        continue;
                    }
                    catch (MQBrokerException e)
                    {
                        if (e.ResponseCode == ResponseCode.ConsumerNotOnline)
                        {
                            mt.TrackType = TrackType.NotOnline;
                            mt.ExceptionDesc = "CODE:" + e.ResponseCode + " DESC:" + e.Message;
                        }
                        else if (e.ResponseCode == ResponseCode.BroadcastConsumption)
                        {
                            mt.TrackType = TrackType.ConsumeBroadcasting;
                        }

                        result.Add(mt);
                        continue;
                    }
                    catch (Exception e)
                    {
                        mt.ExceptionDesc = e.Message;
                        result.Add(mt);
                        continue;
                    }

                    if (ifConsumed)
                    {
                        mt.TrackType = TrackType.Consumed;
                        JsonValue? sub = cc.SubscriptionTable.Find(msg.Topic);
                        if (sub is not null)
                        {
                            JsonValue? tagsSet = sub.Find("tagsSet");
                            bool empty = tagsSet is not { IsArray: true } || tagsSet.Size() == 0;
                            bool wildcard = tagsSet is { IsArray: true }
                                            && Enumerable.Range(0, tagsSet.Size())
                                                .Any(i => tagsSet.At(i).IsString
                                                          && tagsSet.At(i).StringValue() == "*");
                            bool hasTag = msg.Tags is string tag
                                          && tagsSet is { IsArray: true }
                                          && Enumerable.Range(0, tagsSet.Size())
                                              .Any(i => tagsSet.At(i).IsString
                                                        && tagsSet.At(i).StringValue() == tag);
                            if (!empty && !wildcard && !hasTag)
                            {
                                mt.TrackType = TrackType.ConsumedButFiltered;
                            }
                        }
                    }
                    else
                    {
                        mt.TrackType = TrackType.NotConsumeYet;
                    }

                    break;
            }

            result.Add(mt);
        }

        return result;
    }

    public TopicList QueryTopicsByConsumerToBroker(string brokerAddr, string group)
    {
        PropertyMap ext = new() { ["group"] = group };
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.QueryTopicsByConsumer, ext, null, false, _timeoutMillis);
        var tl = new TopicList();
        if (response.Body.Length > 0) TopicList.Decode(response.Body, out tl);
        return tl;
    }

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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.QuerySubscriptionByConsumer, ext, null, false, _timeoutMillis);
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.InvokeBrokerToGetConsumerStatus, ext, null, false, _timeoutMillis);
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
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.CloneGroupOffset, ext, null, false, _timeoutMillis);
    }

    // ---------------------------------------------------------------- Offset management

    public long MaxOffset(MessageQueue mq) => RequireClient().GetMaxOffset(mq);

    public long MinOffset(MessageQueue mq) => RequireClient().GetMinOffset(mq);

    public long SearchOffset(MessageQueue mq, long timestamp) =>
        SearchLowerBoundaryOffset(mq, timestamp);

    public long SearchLowerBoundaryOffset(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByBoundary(mq, timestamp, BoundaryType.Lower);

    public long SearchUpperBoundaryOffset(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByBoundary(mq, timestamp, BoundaryType.Upper);

    public long EarliestMsgStoreTime(MessageQueue mq)
    {
        MQClientInstance client = RequireClient();
        string addr = PublishAddrForMq(client, mq);
        PropertyMap ext = new()
        {
            ["topic"] = mq.Topic,
            ["queueId"] = I64Str(mq.QueueId),
            ["brokerName"] = mq.BrokerName,
        };
        RemotingCommand response = client.InvokeSync(AddrForRpc(addr), RequestCode.GetEarliestMsgStoretime, ext, null, false, _timeoutMillis);
        return ExtInt(response, "timestamp", 0);
    }

    /// <summary>Returns false when the group has no offset on this queue yet (broker replies QUERY_NOT_FOUND).</summary>
    public bool ExamineConsumerOffset(string consumerGroup, MessageQueue mq, out long outOffset) =>
        RequireClient().QueryConsumerOffset(consumerGroup, mq, out outOffset);

    public void UpdateConsumerOffset(string consumerGroup, MessageQueue mq, long offset) =>
        RequireClient().UpdateConsumerOffset(consumerGroup, mq, offset);

    public void UpdateConsumerOffsetToBroker(string brokerAddr, string consumerGroup,
        MessageQueue mq, long offset) =>
        RequireClient().UpdateConsumerOffset(consumerGroup, mq, offset, 5000, brokerAddr);

    /// Note: this no longer uses the old local implementation of "per-queue searchOffset +
    /// updateConsumerOffset" — that does not sync online consumers and performs no
    /// broker-side consistency check.
    public SortedDictionary<MessageQueue, long> ResetOffsetByTimestamp(string topic, string group,
        long timestamp, bool isForce = true, string clusterName = "", bool isCpp = false)
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
    /// A single INVOKE_BROKER_TO_RESET_OFFSET(222), returning the queue offset table the broker
    /// actually reset.
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
            ["isForce"] = isForce ? "true" : "false",
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

    public SortedDictionary<MessageQueue, long> ResetOffsetByQueueId(string brokerAddr,
        string consumerGroup, string topic, int queueId, long resetOffset)
    {
        UpdateConsumerOffsetToBroker(brokerAddr, consumerGroup,
            new MessageQueue(topic, string.Empty, queueId), resetOffset);
        return InvokeBrokerToResetOffset(brokerAddr, topic, consumerGroup, 0, false, false,
            queueId, resetOffset);
    }

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

    // ---------------------------------------------------------------- Message query

    public List<MessageExt> QueryMessage(string topic, string key, int maxNum, long begin, long end) =>
        RequireClient().QueryMessageAllBrokers(
            topic, key, maxNum, begin, end, MessageConst.IndexKeyType, false);

    /// <summary>
    /// Returns false when not hit (note: with the default file index, uniqKey queries need the
    /// RocksDB index).
    /// </summary>
    public bool QueryMessageByUniqKey(string topic, string uniqKey, out MessageExt outMsg)
    {
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
                AddrForRpc(ip + ":" + port.ToString(CultureInfo.InvariantCulture)),
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

    public ConsumeMessageDirectlyResult ConsumeMessageDirectly(string consumerGroup, string clientId,
        string topic, string msgId)
    {
        MessageExt msg = ViewMessage(topic, msgId);
        string outMsgId = msgId;
        if (msg.Properties.TryGetValue(MessageConst.PropertyUniqClientMessageIdKeyidx, out string? uniq)
            && !string.IsNullOrEmpty(uniq)
            && !string.IsNullOrEmpty(msg.OffsetMsgId))
        {
            outMsgId = msg.OffsetMsgId;
        }

        PropertyMap ext = new()
        {
            ["consumerGroup"] = consumerGroup,
            ["clientId"] = clientId,
            ["msgId"] = outMsgId,
            ["topic"] = topic,
        };
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(msg.StoreHostString),
            RequestCode.ConsumeMessageDirectly, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            throw new MQClientException("no consume result for client " + clientId);
        }

        if (!ConsumeMessageDirectlyResult.Decode(response.Body, out ConsumeMessageDirectlyResult result))
        {
            throw new MQClientException("bad ConsumeMessageDirectlyResult body for client " + clientId);
        }

        return result;
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
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.QueryConsumeQueue, ext, null, false, _timeoutMillis);
        var body = new QueryConsumeQueueResponseBody();
        if (response.Body.Length > 0) QueryConsumeQueueResponseBody.Decode(response.Body, out body);
        return body;
    }


    public void CreateAndUpdateTopicConfigList(string brokerAddr, List<TopicConfig> configs)
    {
        if (configs is not { Count: > 0 })
        {
            throw new MQClientException("createAndUpdateTopicConfigList: empty topicConfigList");
        }

        JsonValue rows = JsonValue.MakeArray();
        foreach (TopicConfig config in configs) rows.PushArray(config.ToJson());
        JsonValue body = JsonValue.MakeObject();
        body.Set("topicConfigList", rows);
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.UpdateAndCreateTopicList,
            new PropertyMap(), RemotingSerializable.Encode(body), true, _timeoutMillis);
    }

    public void CreateAndUpdateSubscriptionGroupConfigList(string brokerAddr,
        List<SubscriptionGroupConfig> configs)
    {
        if (configs is not { Count: > 0 })
        {
            throw new MQClientException(
                "createAndUpdateSubscriptionGroupConfigList: empty groupConfigList");
        }

        JsonValue rows = JsonValue.MakeArray();
        foreach (SubscriptionGroupConfig config in configs) rows.PushArray(config.ToJson());
        JsonValue body = JsonValue.MakeObject();
        body.Set("groupConfigList", rows);
        RequireClient().InvokeSync(AddrForRpc(brokerAddr),
            RequestCode.UpdateAndCreateSubscriptiongroupList,
            new PropertyMap(), RemotingSerializable.Encode(body), true, _timeoutMillis);
    }

    public void CreateStaticTopic(string brokerAddr, string defaultTopic, TopicConfig config,
        JsonValue mappingDetail, bool force = false)
    {
        PropertyMap ext = new()
        {
            ["topic"] = config.TopicName,
            ["defaultTopic"] = defaultTopic,
            ["readQueueNums"] = I64Str(config.ReadQueueNums),
            ["writeQueueNums"] = I64Str(config.WriteQueueNums),
            ["perm"] = I64Str(config.Perm),
            ["topicFilterType"] = config.TopicFilterType.Length == 0
                ? TopicFilterType.SingleTag
                : config.TopicFilterType,
            ["topicSysFlag"] = I64Str(config.TopicSysFlag),
            ["order"] = config.Order ? "true" : "false",
            ["force"] = force ? "true" : "false",
        };
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.UpdateAndCreateStaticTopic,
            ext, RemotingSerializable.Encode(mappingDetail), true, _timeoutMillis);
    }

    public GroupForbidden UpdateAndGetGroupReadForbidden(string brokerAddr, string group,
        string topic, bool? readable = null)
    {
        if (group.Length == 0 || topic.Length == 0)
        {
            throw new MQClientException("updateAndGetGroupReadForbidden: group/topic required");
        }

        PropertyMap ext = new() { ["group"] = group, ["topic"] = topic };
        if (readable is not null) ext["readable"] = readable.Value ? "true" : "false";
        RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(brokerAddr),
            RequestCode.UpdateAndGetGroupForbidden, ext, null, false, _timeoutMillis);
        if (response.Body.Length == 0)
        {
            throw new MQClientException("updateAndGetGroupReadForbidden: empty response body");
        }

        if (!GroupForbidden.Decode(response.Body, out GroupForbidden outValue))
        {
            throw new MQClientException("updateAndGetGroupReadForbidden: bad GroupForbidden body");
        }

        return outValue;
    }

    public bool ResumeCheckHalfMessage(string brokerAddr, string topic, string msgId = "")
    {
        if (topic.Length == 0)
        {
            throw new MQClientException("resumeCheckHalfMessage: topic required");
        }

        PropertyMap ext = new() { ["topic"] = topic };
        if (msgId.Length > 0) ext["msgId"] = msgId;
        RemotingCommand response = RequireClient().InvokeSyncRaw(AddrForRpc(brokerAddr),
            RequestCode.ResumeCheckHalfMessage, ext, null, false, _timeoutMillis);
        return response.Code == ResponseCode.Success;
    }

    public void CreateOrUpdateOrderConf(string key, string value, bool isCluster = false)
    {
        if (key.Length == 0 || value.Length == 0)
        {
            throw new MQClientException("createOrUpdateOrderConf: key/value required");
        }

        if (isCluster)
        {
            PutKvConfig(MixAll.NamespaceOrderTopicConfig, key, value);
            return;
        }

        string oldValue = string.Empty;
        try
        {
            GetKvConfig(MixAll.NamespaceOrderTopicConfig, key, out oldValue);
        }
        catch (Exception)
        {
            oldValue = string.Empty;
        }

        var entries = new Dictionary<string, string>();
        foreach (string raw in oldValue.Split(';'))
        {
            string entry = raw.Trim();
            if (entry.Length == 0) continue;
            int colon = entry.IndexOf(':');
            string entryKey = colon >= 0 ? entry[..colon] : entry;
            entries[entryKey] = entry;
        }

        int sep = value.IndexOf(':');
        string newKey = sep >= 0 ? value[..sep] : value;
        if (newKey.Length == 0)
        {
            throw new MQClientException("createOrUpdateOrderConf: value must start with a key");
        }

        entries[newKey] = value;
        PutKvConfig(MixAll.NamespaceOrderTopicConfig, key, string.Join(";", entries.Values));
    }


    /// <summary>CLEAN_EXPIRED_CONSUMEQUEUE(306): the broker drops consume-queue entries older than <c>timeHours</c> hours.</summary>
    public void CleanExpiredConsumerQueue(string brokerAddr, int timeHours)
    {
        PropertyMap ext = new() { ["time"] = I64Str(timeHours) };
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.CleanExpiredConsumequeue,
            ext, null, false, _timeoutMillis);
    }

    public List<string> CleanExpiredConsumerQueueByAddr(List<string> addrs, int timeHours)
    {
        var failed = new List<string>();
        foreach (string addr in addrs)
        {
            try
            {
                CleanExpiredConsumerQueue(addr, timeHours);
            }
            catch (Exception)
            {
                failed.Add(addr);
            }
        }

        return failed;
    }

    /// <summary>DELETE_EXPIRED_COMMITLOG(329): the broker deletes commit-log files older than <c>timeHours</c> hours.</summary>
    public void DeleteExpiredCommitLog(string brokerAddr, int timeHours)
    {
        PropertyMap ext = new() { ["time"] = I64Str(timeHours) };
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.DeleteExpiredCommitlog,
            ext, null, false, _timeoutMillis);
    }

    /// <summary>Multi-address form of DeleteExpiredCommitLog.</summary>
    public List<string> DeleteExpiredCommitLogByAddr(List<string> addrs, int timeHours)
    {
        var failed = new List<string>();
        foreach (string addr in addrs)
        {
            try
            {
                DeleteExpiredCommitLog(addr, timeHours);
            }
            catch (Exception)
            {
                failed.Add(addr);
            }
        }

        return failed;
    }

    public void CleanUnusedTopicByAddr(string brokerAddr)
    {
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.CleanUnusedTopic,
            new PropertyMap(), null, false, _timeoutMillis);
    }

    public JsonValue QueryConsumeTimeSpan(string topic, string group)
    {
        TopicRouteData route = ExamineTopicRoute(topic);
        JsonValue spans = JsonValue.MakeArray();
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0) continue;
            PropertyMap ext = new() { ["topic"] = topic, ["group"] = group };
            RemotingCommand response = RequireClient().InvokeSync(AddrForRpc(addr),
                RequestCode.QueryConsumeTimeSpan, ext, null, false, _timeoutMillis);
            if (response.Body.Length == 0) continue;
            if (!RemotingSerializable.Decode(response.Body, out JsonValue v)) continue;
            JsonValue? arr = v.Find("consumeTimeSpanSet");
            if (arr is { IsArray: true })
            {
                for (int i = 0; i < arr.Size(); ++i) spans.PushArray(arr.At(i));
            }
        }

        return spans;
    }

    public HashSet<string> GetTopicClusterList(string topic) => GetClusterList(topic);

    public void SetMessageRequestMode(string brokerAddr, string topic, string consumerGroup,
        string mode, int popShareQueueNum = 0)
    {
        PropertyMap ext = new()
        {
            ["topic"] = topic,
            ["consumerGroup"] = consumerGroup,
            ["mode"] = mode,
        };
        if (popShareQueueNum > 0) ext["popShareQueueNum"] = I64Str(popShareQueueNum);
        RequireClient().InvokeSync(AddrForRpc(brokerAddr), RequestCode.SetMessageRequestMode,
            ext, null, false, _timeoutMillis);
    }


    public void UpdateNameServerConfig(PropertyMap properties, int timeoutMillis = -1)
    {
        string text = MixAll.Properties2String(properties ?? new PropertyMap());
        if (text.Length == 0) return;
        MQClientInstance client = RequireClient();
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        bool anyFailed = false;
        int lastCode = 0;
        string lastRemark = string.Empty;
        foreach (string nsAddr in client.NameServerAddrs)
        {
            RemotingCommand response = client.InvokeSyncRaw(
                nsAddr, RequestCode.UpdateNamesrvConfig, new PropertyMap(),
                Encoding.UTF8.GetBytes(text), true, to);
            if (response.Code != ResponseCode.Success)
            {
                anyFailed = true;
                lastCode = response.Code;
                lastRemark = response.Remark;
            }
        }

        if (anyFailed)
        {
            throw new MQClientException(
                lastRemark.Length == 0 ? "update name server config failed" : lastRemark, lastCode);
        }
    }

    public Dictionary<string, PropertyMap> GetNameServerConfig(List<string>? namesrvAddrs = null,
        int timeoutMillis = -1)
    {
        MQClientInstance client = RequireClient();
        List<string> targets = namesrvAddrs is { Count: > 0 }
            ? namesrvAddrs
            : new List<string>(client.NameServerAddrs);
        int to = timeoutMillis < 0 ? _timeoutMillis : timeoutMillis;
        var result = new Dictionary<string, PropertyMap>();
        foreach (string nsAddr in targets)
        {
            RemotingCommand response = client.InvokeSync(
                nsAddr, RequestCode.GetNamesrvConfig, new PropertyMap(), null, false, to);
            result[nsAddr] = MixAll.String2Properties(Encoding.UTF8.GetString(response.Body));
        }

        return result;
    }
}
