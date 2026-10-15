// Purpose: the client sends GET_ROUTEINFO_BY_TOPIC to the NameServer, and the response body
// is the JSON of TopicRouteData. From it the client learns the cluster's broker addresses
// (brokerDatas) and each topic's read/write queue counts (queueDatas), then assembles the
// list of MessageQueues that can be sent to / pulled from.
//
// JSON fields (consistent with fastjson2, keys in alphabetical order):
//   QueueData      : brokerName | perm | readQueueNums | topicSysFlag | writeQueueNums
//   BrokerData     : brokerAddrs | brokerName | cluster | enableActingMaster | zoneName
//   TopicRouteData : brokerDatas | filterServerTable | orderTopicConf | queueDatas
//                    (+ topicQueueMappingByBroker, output only when non-null)
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public sealed class QueueData : IComparable<QueueData>
{
    public string BrokerName { get; set; } = string.Empty;
    public int ReadQueueNums { get; set; }
    public int WriteQueueNums { get; set; }
    public int Perm { get; set; }
    public int TopicSysFlag { get; set; }

    public QueueData()
    {
    }

    public QueueData(string brokerName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag)
    {
        BrokerName = brokerName;
        ReadQueueNums = readQueueNums;
        WriteQueueNums = writeQueueNums;
        Perm = perm;
        TopicSysFlag = topicSysFlag;
    }

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        o.Set("brokerName", JsonValue.MakeString(BrokerName));
        o.Set("perm", JsonValue.MakeInt(Perm));
        o.Set("readQueueNums", JsonValue.MakeInt(ReadQueueNums));
        o.Set("topicSysFlag", JsonValue.MakeInt(TopicSysFlag));
        o.Set("writeQueueNums", JsonValue.MakeInt(WriteQueueNums));
        return o;
    }

    public static QueueData FromJson(JsonValue v)
    {
        var q = new QueueData();
        if (!v.IsObject)
        {
            return q;
        }

        if (v.TryGetString("brokerName", out string s))
        {
            q.BrokerName = s;
        }

        if (v.TryGetInt("perm", out long n))
        {
            q.Perm = LegacyNumber.ToInt32(n);
        }

        if (v.TryGetInt("readQueueNums", out n))
        {
            q.ReadQueueNums = LegacyNumber.ToInt32(n);
        }

        if (v.TryGetInt("writeQueueNums", out n))
        {
            q.WriteQueueNums = LegacyNumber.ToInt32(n);
        }

        if (v.TryGetInt("topicSysFlag", out n))
        {
            q.TopicSysFlag = LegacyNumber.ToInt32(n);
        }

        return q;
    }

    public bool Equals(QueueData? other) =>
        other is not null && BrokerName == other.BrokerName && Perm == other.Perm
        && ReadQueueNums == other.ReadQueueNums && WriteQueueNums == other.WriteQueueNums
        && TopicSysFlag == other.TopicSysFlag;

    public override bool Equals(object? obj) => obj is QueueData other && Equals(other);

    public int CompareTo(QueueData? other)
    {
        if (other is null)
        {
            return 1;
        }

        if (BrokerName == other.BrokerName)
        {
            return 0;
        }

        return string.CompareOrdinal(BrokerName, other.BrokerName) < 0 ? -1 : 1;
    }

    public static bool operator ==(QueueData? left, QueueData? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(QueueData? left, QueueData? right) => !(left == right);

    public override int GetHashCode()
    {
        long result = 1;
        result = 31 * result + LegacyHash.StringHash(BrokerName);
        result = 31 * result + Perm;
        result = 31 * result + ReadQueueNums;
        result = 31 * result + WriteQueueNums;
        result = 31 * result + TopicSysFlag;
        result &= 0xFFFFFFFFL;
        return unchecked((int)result);
    }

    public override string ToString() =>
        "QueueData [brokerName=" + BrokerName + ", readQueueNums=" + ReadQueueNums.ToString(CultureInfo.InvariantCulture)
        + ", writeQueueNums=" + WriteQueueNums.ToString(CultureInfo.InvariantCulture)
        + ", perm=" + Perm.ToString(CultureInfo.InvariantCulture)
        + ", topicSysFlag=" + TopicSysFlag.ToString(CultureInfo.InvariantCulture) + "]";
}

public sealed class BrokerData : IComparable<BrokerData>
{
    public string Cluster { get; set; } = string.Empty;
    public string BrokerName { get; set; } = string.Empty;

    // brokerId -> address. brokerId=0 is the master (MixAll.MASTER_ID)
    public SortedDictionary<long, string> BrokerAddrs { get; set; } = new();

    public string ZoneName { get; set; } = string.Empty;
    public bool EnableActingMaster { get; set; }

    public BrokerData()
    {
    }

    public BrokerData(string cluster, string brokerName,
        SortedDictionary<long, string>? brokerAddrs, string zoneName = "", bool enableActingMaster = false)
    {
        Cluster = cluster;
        BrokerName = brokerName;
        if (brokerAddrs is not null)
        {
            BrokerAddrs = brokerAddrs;
        }

        ZoneName = zoneName;
        EnableActingMaster = enableActingMaster;
    }

    /// <summary>
    /// Prefers brokerId=0 (master); otherwise takes a random slave address. Returns an empty
    /// string when no address is available.
    /// </summary>
    public string SelectBrokerAddr()
    {
        if (BrokerAddrs.Count == 0)
        {
            return string.Empty;
        }

        if (BrokerAddrs.TryGetValue(0, out string? master)) // MixAll.MASTER_ID
        {
            return master;
        }

        int idx = ThreadLocalRandom.Next(BrokerAddrs.Count);
        int i = 0;
        foreach (var kv in BrokerAddrs)
        {
            if (i++ == idx)
            {
                return kv.Value;
            }
        }

        return BrokerAddrs.Values.First();
    }

    // fastjson2's brokerAddrs is a HashMap<Long,String> with numeric keys.
    // We output string keys ("0") as valid JSON — semantically equivalent and readable by
    // any parser.
    private static JsonValue AddrsToJson(SortedDictionary<long, string> addrs)
    {
        var o = JsonValue.MakeObject();
        foreach (var kv in addrs)
        {
            o.Set(kv.Key.ToString(CultureInfo.InvariantCulture), JsonValue.MakeString(kv.Value));
        }

        return o;
    }

    private static SortedDictionary<long, string> AddrsFromJson(JsonValue v)
    {
        var @out = new SortedDictionary<long, string>();
        if (!v.IsObject)
        {
            return @out;
        }

        foreach (var kv in v.ObjectItems())
        {
            // The key may be "0" (standard JSON) or 0 (a fastjson bare numeric key parsed as a string)
            long id = 0;
            if (kv.Key.Length > 0)
            {
                bool numeric = kv.Key.All(c => c is >= '0' and <= '9');
                if (!numeric)
                {
                    continue; // non-numeric key (bad data) is skipped
                }

                id = long.Parse(kv.Key, CultureInfo.InvariantCulture);
            }

            @out[id] = kv.Value.StringValue();
        }

        return @out;
    }

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        o.Set("brokerAddrs", AddrsToJson(BrokerAddrs));
        o.Set("brokerName", JsonValue.MakeString(BrokerName));
        o.Set("cluster", JsonValue.MakeString(Cluster));
        o.Set("enableActingMaster", JsonValue.MakeBool(EnableActingMaster));
        o.Set("zoneName", JsonValue.MakeString(ZoneName));
        return o;
    }

    public static BrokerData FromJson(JsonValue v)
    {
        var b = new BrokerData();
        if (!v.IsObject)
        {
            return b;
        }

        if (v.TryGetString("brokerName", out string s))
        {
            b.BrokerName = s;
        }

        if (v.TryGetString("cluster", out s))
        {
            b.Cluster = s;
        }

        if (v.TryGetString("zoneName", out s))
        {
            b.ZoneName = s;
        }

        if (v.TryGetBool("enableActingMaster", out bool bv))
        {
            b.EnableActingMaster = bv;
        }

        JsonValue? addrs = v.Find("brokerAddrs");
        if (addrs is not null)
        {
            b.BrokerAddrs = AddrsFromJson(addrs);
        }

        return b;
    }

    public bool Equals(BrokerData? other) =>
        other is not null && Cluster == other.Cluster && BrokerName == other.BrokerName
        && BrokerAddrs.SequenceEqual(other.BrokerAddrs);

    public override bool Equals(object? obj) => obj is BrokerData other && Equals(other);

    public int CompareTo(BrokerData? other)
    {
        if (other is null)
        {
            return 1;
        }

        if (BrokerName == other.BrokerName)
        {
            return 0;
        }

        return string.CompareOrdinal(BrokerName, other.BrokerName) < 0 ? -1 : 1;
    }

    public static bool operator ==(BrokerData? left, BrokerData? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(BrokerData? left, BrokerData? right) => !(left == right);

    public override int GetHashCode()
    {
        long result = 1;
        result = 31 * result + LegacyHash.StringHash(Cluster);
        result = 31 * result + LegacyHash.StringHash(BrokerName);
        // Map.hashCode = sum of (keyHash ^ valueHash) over entries
        long mapHash = 0;
        foreach (var kv in BrokerAddrs)
        {
            long k = kv.Key; // Long.hashCode = (int)(v ^ (v>>>32))
            int kh = unchecked((int)(k ^ (long)((ulong)k >> 32)));
            mapHash += kh ^ LegacyHash.StringHash(kv.Value);
            mapHash &= 0xFFFFFFFFL;
        }

        result = 31 * result + mapHash;
        result &= 0xFFFFFFFFL;
        return unchecked((int)result);
    }

    public override string ToString()
    {
        string addrs = "{";
        bool first = true;
        foreach (var kv in BrokerAddrs)
        {
            if (!first)
            {
                addrs += ", ";
            }

            first = false;
            addrs += kv.Key.ToString(CultureInfo.InvariantCulture) + "=" + kv.Value;
        }

        addrs += "}";
        return "BrokerData [brokerName=" + BrokerName + ", brokerAddrs=" + addrs + "]";
    }
}

public sealed class TopicRouteData
{
    public string OrderTopicConf { get; set; } = string.Empty;
    public List<QueueData> QueueDatas { get; set; } = new();
    public List<BrokerData> BrokerDatas { get; set; } = new();
    public SortedDictionary<string, List<string>> FilterServerTable { get; set; } = new();

    // pass-through: TopicQueueMappingInfo is not modeled; the JSON is kept verbatim to avoid
    // dropping fields. IsNull() means the field is absent.
    public JsonValue TopicQueueMappingByBroker { get; set; } = JsonValue.Null;

    /// <summary>
    /// Assembles all **writable** MessageQueues from queueDatas + brokerDatas
    /// (corresponds to the assembly logic of MQClientInstance.topicRouteData2TopicPublishInfo).
    /// topic is backfilled into every MessageQueue; otherwise later route lookups by mq.topic
    /// would find nothing.
    public List<MessageQueue> GetAllMessageQueue(string topic)
    {
        var mqs = new List<MessageQueue>();
        foreach (QueueData qd in QueueDatas)
        {
            if (!PermName.CheckPerm(qd.Perm, PermName.PermWrite))
            {
                continue;
            }

            BrokerData? found = BrokerDatas.FirstOrDefault(bd => bd.BrokerName == qd.BrokerName);
            if (found is null)
            {
                continue;
            }

            if (!found.BrokerAddrs.ContainsKey(MixAll.MasterId))
            {
                continue;
            }

            for (int i = 0; i < qd.WriteQueueNums; ++i)
            {
                mqs.Add(new MessageQueue(topic, qd.BrokerName, i));
            }
        }

        return mqs;
    }

    public List<MessageQueue> GetAllSubscribeMessageQueue(string topic)
    {
        var mqs = new List<MessageQueue>();
        foreach (QueueData qd in QueueDatas)
        {
            if (!PermName.CheckPerm(qd.Perm, PermName.PermRead))
            {
                continue;
            }

            for (int i = 0; i < qd.ReadQueueNums; ++i)
            {
                mqs.Add(new MessageQueue(topic, qd.BrokerName, i));
            }
        }

        return mqs;
    }

    public bool TopicRouteDataChanged(TopicRouteData? oldData)
    {
        if (oldData is null)
        {
            return true;
        }

        var nowQ = new List<QueueData>(QueueDatas);
        var oldQ = new List<QueueData>(oldData.QueueDatas);
        var nowB = new List<BrokerData>(BrokerDatas);
        var oldB = new List<BrokerData>(oldData.BrokerDatas);
        nowQ.Sort();
        oldQ.Sort();
        nowB.Sort();
        oldB.Sort();
        return !(nowQ.SequenceEqual(oldQ) && nowB.SequenceEqual(oldB));
    }

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        var bArr = JsonValue.MakeArray();
        foreach (BrokerData b in BrokerDatas)
        {
            bArr.PushArray(b.ToJson());
        }

        o.Set("brokerDatas", bArr);

        var fst = JsonValue.MakeObject();
        foreach (var kv in FilterServerTable)
        {
            var arr = JsonValue.MakeArray();
            foreach (string s in kv.Value)
            {
                arr.PushArray(JsonValue.MakeString(s));
            }

            fst.Set(kv.Key, arr);
        }

        o.Set("filterServerTable", fst);

        o.Set("orderTopicConf", JsonValue.MakeString(OrderTopicConf));

        var qArr = JsonValue.MakeArray();
        foreach (QueueData q in QueueDatas)
        {
            qArr.PushArray(q.ToJson());
        }

        o.Set("queueDatas", qArr);

        if (!TopicQueueMappingByBroker.IsNull)
        {
            o.Set("topicQueueMappingByBroker", TopicQueueMappingByBroker);
        }

        return o;
    }

    public static TopicRouteData FromJson(JsonValue v)
    {
        var t = new TopicRouteData();
        if (!v.IsObject)
        {
            return t;
        }

        if (v.TryGetString("orderTopicConf", out string s))
        {
            t.OrderTopicConf = s;
        }

        JsonValue? qs = v.Find("queueDatas");
        if (qs is { IsArray: true })
        {
            for (int i = 0; i < qs.Size(); ++i)
            {
                t.QueueDatas.Add(QueueData.FromJson(qs.At(i)));
            }
        }

        JsonValue? bs = v.Find("brokerDatas");
        if (bs is { IsArray: true })
        {
            for (int i = 0; i < bs.Size(); ++i)
            {
                t.BrokerDatas.Add(BrokerData.FromJson(bs.At(i)));
            }
        }

        JsonValue? fst = v.Find("filterServerTable");
        if (fst is { IsObject: true })
        {
            foreach (var kv in fst.ObjectItems())
            {
                var servers = new List<string>();
                if (kv.Value.IsArray)
                {
                    for (int i = 0; i < kv.Value.Size(); ++i)
                    {
                        servers.Add(kv.Value.At(i).StringValue());
                    }
                }

                t.FilterServerTable[kv.Key] = servers;
            }
        }

        JsonValue? tqm = v.Find("topicQueueMappingByBroker");
        if (tqm is { IsNull: false })
        {
            t.TopicQueueMappingByBroker = tqm;
        }

        return t;
    }

    public bool Equals(TopicRouteData? other) =>
        other is not null && BrokerDatas.SequenceEqual(other.BrokerDatas)
        && OrderTopicConf == other.OrderTopicConf && QueueDatas.SequenceEqual(other.QueueDatas)
        && FilterServerTable.SequenceEqual(other.FilterServerTable)
        && string.Equals(TopicQueueMappingByBroker.Dump(), other.TopicQueueMappingByBroker.Dump(), StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is TopicRouteData other && Equals(other);

    public override int GetHashCode() => BrokerNameHash();

    private int BrokerNameHash() => LegacyHash.StringHash(OrderTopicConf);

    // equivalent of RemotingSerializable.encode / decode
    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out TopicRouteData @out)
    {
        @out = new TopicRouteData();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    public override string ToString() =>
        "TopicRouteData [orderTopicConf=" + OrderTopicConf
        + ", queueDatas=" + QueueDatas.Count.ToString(CultureInfo.InvariantCulture)
        + ", brokerDatas=" + BrokerDatas.Count.ToString(CultureInfo.InvariantCulture) + "]";
}

/// <summary>Lightweight thread-safe random source: for use cases needing an independent source per thread.</summary>
internal static class ThreadLocalRandom
{
    private static readonly ThreadLocal<Random> Rng = new(() => new Random(Interlocked.Increment(ref _seed) ^ Environment.TickCount));
    private static int _seed;

    public static int Next(int upperExclusive) => Rng.Value!.Next(upperExclusive);
}
