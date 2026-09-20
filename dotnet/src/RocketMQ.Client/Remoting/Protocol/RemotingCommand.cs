// RemotingCommand：RocketMQ 远程命令（对应 org.apache.rocketmq.remoting.protocol.RemotingCommand）。
//
// 线格式：totalLength(4) | headerLength(高 8 位放序列化类型, 4) | header | body
//
//   header(JSON)      : {"code":..,"language":..,"version":..,"opaque":..,"flag":..,
//                        "remark":"..","extFields":{..}}
//   header(ROCKETMQ)  : code(2) language(1) version(2) opaque(4) flag(4)
//                       remark(int+utf8) extFields(int + [key(short+utf8) value(int+utf8)]...)
using System.Globalization;
using System.Text;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public sealed class RemotingCommand
{
    public const int RpcType = 0;     // 0 -> REQUEST, 1 -> RESPONSE
    public const int RpcOneway = 1;

    /// <summary>
    /// Java <c>MQVersion.CURRENT_VERSION</c>（= <c>Version.V5_5_1</c>.ordinal()，本机 5.5.1 集群）。
    /// broker 按心跳/请求里记录的客户端版本决定能否把管理请求回调到客户端：
    /// <c>AdminBrokerProcessor#callConsumer</c>（307）低于 V3_1_8_SNAPSHOT（ordinal 62）时直接回
    /// "The Consumer &lt;x&gt; Version &lt;0&gt; too low to finish"；<c>Broker2Client#getConsumeStatus</c>
    /// （223 推 221）低于 V3_0_7_SNAPSHOT（ordinal 28）时回 "the client does not support this
    /// feature. version=..."，resetOffset 的在线分支同样按 28 跳过。发 0 等于自降为不可回调。
    /// </summary>
    public const int CurrentVersion = 515;

    private static readonly object SerializeTypeLock = new();
    private static int _opaqueCounter;
    private static byte _serializeTypeConfig = LoadSerializeTypeConfig();
    private static readonly int VersionConfig = LoadVersionConfig();

    public int Code { get; set; }

    /// <summary>.NET 客户端用 DOTNET(2)，而不是 C++ 侧的 CPP(1)。</summary>
    public byte Language { get; set; } = LanguageCode.Dotnet;

    public int Version { get; set; }

    /// <summary>
    /// ⚠ opaque 从 0 起算，第一个请求的合法值就是 0——**不能用 0 当「未设置」哨兵**。
    /// 传输层只在真的与在途请求冲突时才重分配（见 RemotingClient）。
    /// </summary>
    public int Opaque { get; set; }

    public int Flag { get; set; }

    public string Remark { get; set; } = string.Empty;

    public bool HasRemark { get; set; }

    public PropertyMap ExtFields { get; set; } = new();

    public ICommandCustomHeader? CustomHeader { get; set; }

    public byte[] Body { get; set; } = Array.Empty<byte>();

    public bool HasBody { get; set; }

    public byte SerializeTypeCurrentRpc { get; set; } = SerializeType.Json;

    public RemotingCommand()
    {
    }

    private RemotingCommand(
        int code, ICommandCustomHeader? header, string remark, int opaque, int flag, byte[] body)
    {
        Code = code;
        Language = LanguageCode.Dotnet;
        Opaque = opaque;
        Flag = flag;
        Remark = remark;
        HasRemark = remark.Length > 0;
        CustomHeader = header;
        Body = body;
        HasBody = true;
        SerializeTypeCurrentRpc = GetSerializeTypeConfig();
    }

    // ---------------- 工厂方法 ----------------

    public static RemotingCommand CreateRequestCommand(
        int code, ICommandCustomHeader? header = null)
    {
        var cmd = new RemotingCommand
        {
            Code = code,
            CustomHeader = header,
            Opaque = NextOpaque(),
            SerializeTypeCurrentRpc = GetSerializeTypeConfig(),
            Version = VersionConfig,
        };
        return cmd;
    }

    public static RemotingCommand CreateResponseCommandWithHeader(
        int code, ICommandCustomHeader? header = null)
    {
        var cmd = new RemotingCommand
        {
            Code = code,
            CustomHeader = header,
            SerializeTypeCurrentRpc = GetSerializeTypeConfig(),
            Version = VersionConfig,
        };
        cmd.MarkResponseType();
        return cmd;
    }

    public static RemotingCommand CreateResponseCommand(int code, string? remark)
    {
        var cmd = new RemotingCommand
        {
            Code = code,
            Remark = remark ?? string.Empty,
            HasRemark = remark is { Length: > 0 },
            SerializeTypeCurrentRpc = GetSerializeTypeConfig(),
            Version = VersionConfig,
        };
        cmd.MarkResponseType();
        return cmd;
    }

    public static RemotingCommand BuildErrorResponse(int code, string remark) =>
        CreateResponseCommand(code, remark);

    /// <summary>全局是否为本次 RPC 使用 ROCKETMQ 二进制序列化。</summary>
    public static void SetSerializeTypeConfig(byte type)
    {
        lock (SerializeTypeLock)
        {
            _serializeTypeConfig = type;
        }
    }

    public static byte GetSerializeTypeConfig()
    {
        lock (SerializeTypeLock)
        {
            return _serializeTypeConfig;
        }
    }

    // ---------------- 帧工具 ----------------

    /// <summary>Java getProtocolType：取首字节（这里按整型高 8 位同理）。</summary>
    public static byte GetProtocolType(int source) =>
        (byte)((unchecked((uint)source) >> 24) & 0xFF);

    public static int GetHeaderLength(int length) => length & 0xFFFFFF;

    public static int MarkProtocolType(int source, byte type) =>
        unchecked((int)(((uint)type << 24) | ((uint)source & 0x00FFFFFFu)));

    public void MarkResponseType() => Flag |= 1 << RpcType;

    public bool IsResponseType() => (Flag & (1 << RpcType)) == (1 << RpcType);

    public void MarkOnewayRpc() => Flag |= 1 << RpcOneway;

    public bool IsOnewayRpc() => (Flag & (1 << RpcOneway)) == (1 << RpcOneway);

    public string GetCommandType() => IsResponseType()
        ? RemotingCommandType.ResponseCommand
        : RemotingCommandType.RequestCommand;

    // ---------------- 编解码 ----------------

    /// <summary>把 customHeader 的非空字段展开到 extFields（对应 Java 的反射行为）。</summary>
    public void MakeCustomHeaderToNet()
    {
        if (CustomHeader is null)
        {
            return;
        }

        foreach (var kv in CustomHeader.ToExtFields())
        {
            ExtFields[kv.Key] = kv.Value;
        }
    }

    public JsonValue ToJsonObject()
    {
        var obj = JsonValue.MakeObject();
        obj.Set("code", JsonValue.MakeInt(Code));
        obj.Set("language", JsonValue.MakeInt(Language));
        obj.Set("version", JsonValue.MakeInt(Version));
        obj.Set("opaque", JsonValue.MakeInt(Opaque));
        obj.Set("flag", JsonValue.MakeInt(Flag));
        if (HasRemark && Remark.Length > 0)
        {
            obj.Set("remark", JsonValue.MakeString(Remark));
        }

        if (ExtFields.Count > 0)
        {
            var ext = JsonValue.MakeObject();
            foreach (var kv in ExtFields)
            {
                ext.Set(kv.Key, JsonValue.MakeString(kv.Value));
            }

            obj.Set("extFields", ext);
        }

        return obj;
    }

    public byte[] HeaderEncode()
    {
        MakeCustomHeaderToNet();
        if (SerializeTypeCurrentRpc == SerializeType.Rocketmq)
        {
            var fields = new ProtocolHeaderFields
            {
                Code = Code,
                Language = Language,
                Version = Version,
                Opaque = Opaque,
                Flag = Flag,
                Remark = Remark,
                HasRemark = HasRemark && Remark.Length > 0,
                ExtFields = ExtFields,
            };
            return RocketMQSerializable.RocketMqProtocolEncode(fields);
        }

        return RemotingSerializable.Encode(ToJsonObject());
    }

    public byte[] Encode()
    {
        int length = 4;
        byte[] headerData = HeaderEncode();
        length += headerData.Length;
        // body 存在判定：显式置 HasBody，或 body 非空（与 Java 的 body != null 语义对齐）
        bool withBody = HasBody || Body.Length > 0;
        if (withBody)
        {
            length += Body.Length;
        }

        var writer = new ByteWriter(32 + headerData.Length + Body.Length);
        writer.WriteInt32(length);
        writer.WriteInt32(MarkProtocolType(headerData.Length, SerializeTypeCurrentRpc));
        writer.WriteBytes(headerData);
        if (withBody)
        {
            writer.WriteBytes(Body);
        }

        return writer.ToArray();
    }

    /// <summary>只编码头部（body 长度另算），用于分帧发送。</summary>
    public byte[] EncodeHeader(int bodyLength = 0)
    {
        int length = 4;
        byte[] headerData = HeaderEncode();
        length += headerData.Length + bodyLength;
        var writer = new ByteWriter(32 + headerData.Length);
        writer.WriteInt32(length);
        writer.WriteInt32(MarkProtocolType(headerData.Length, SerializeTypeCurrentRpc));
        writer.WriteBytes(headerData);
        return writer.ToArray();
    }

    /// <summary>解析失败返回 false（err 为失败原因）。对应 C++ 的 tryDecode。</summary>
    public static bool TryDecode(byte[] data, out RemotingCommand outCmd, out string? error)
    {
        outCmd = new RemotingCommand();
        error = null;
        if (data.Length < 8)
        {
            error = "decode error, data too short";
            return false;
        }

        int totalLength = ByteReader.GetInt32At(data, 0);
        int offset = 4;
        if (totalLength > data.Length - 4)
        {
            error = "decode error, bad total length";
            return false;
        }

        int oriHeaderLen = ByteReader.GetInt32At(data, 4);
        offset += 4;
        int headerLength = GetHeaderLength(oriHeaderLen);
        if (headerLength < 0 || headerLength > data.Length - offset)
        {
            error = "decode error, bad header length";
            return false;
        }

        byte protocolType = GetProtocolType(oriHeaderLen);

        byte[] headerData = data[offset..(offset + headerLength)];
        offset += headerLength;

        var cmd = new RemotingCommand();
        if (protocolType == SerializeType.Rocketmq)
        {
            if (!RocketMQSerializable.RocketMqProtocolDecode(headerData, out ProtocolHeaderFields fields))
            {
                error = "decode error, rocketmq protocol decode failed";
                return false;
            }

            cmd.Code = fields.Code;
            cmd.Remark = fields.Remark;
            cmd.HasRemark = fields.HasRemark;
            cmd.Language = fields.Language;
            cmd.Version = fields.Version;
            cmd.Opaque = fields.Opaque;
            cmd.Flag = fields.Flag;
            cmd.ExtFields = fields.ExtFields;
        }
        else
        {
            if (!Json.TryParse(Encoding.UTF8.GetString(headerData), out JsonValue obj, out _))
            {
                error = "decode error, json parse failed";
                return false;
            }

            cmd.Code = unchecked((int)obj.Get("code").IntValue(0));
            cmd.Remark = obj.Get("remark").StringValue();
            cmd.HasRemark = cmd.Remark.Length > 0;
            cmd.Flag = unchecked((int)obj.Get("flag").IntValue(0));
            JsonValue lang = obj.Get("language");
            if (lang.IsString)
            {
                // 5.x 会把 language 序列化成**枚举名字符串**（4.x 是 int），两者都收
                cmd.Language = LanguageNameToCode(lang.StringValue());
            }
            else
            {
                cmd.Language = unchecked((byte)lang.IntValue(LanguageCode.Java));
            }

            cmd.Version = unchecked((int)obj.Get("version").IntValue(0));
            cmd.Opaque = unchecked((int)obj.Get("opaque").IntValue(-1));
            JsonValue? ext = obj.Find("extFields");
            if (ext is { IsObject: true })
            {
                foreach (var kv in ext.ObjectItems())
                {
                    cmd.ExtFields[kv.Key] = kv.Value.StringValue();
                }
            }
        }

        cmd.SerializeTypeCurrentRpc = protocolType;

        if (data.Length > offset)
        {
            cmd.Body = data[offset..];
            cmd.HasBody = true;
        }
        else
        {
            cmd.Body = Array.Empty<byte>();
            cmd.HasBody = false;
        }

        outCmd = cmd;
        return true;
    }

    /// <summary>解析失败抛 <see cref="RemotingCommandException"/>。</summary>
    public static RemotingCommand Decode(byte[] data)
    {
        if (!TryDecode(data, out RemotingCommand cmd, out string? err))
        {
            throw new RemotingCommandException(string.IsNullOrEmpty(err) ? "decode error" : err);
        }

        return cmd;
    }

    /// <summary>把 extFields 映射回自定义头部对象。</summary>
    public void DecodeCommandCustomHeader(ICommandCustomHeader header) =>
        header.FromExtFields(ExtFields);

    public void AddExtField(string key, string value) => ExtFields[key] = value;

    public string GetExtField(string key) =>
        ExtFields.TryGetValue(key, out string? v) ? v : string.Empty;

    public override string ToString()
    {
        var ext = new StringBuilder();
        foreach (var kv in ExtFields)
        {
            if (ext.Length > 0)
            {
                ext.Append(',');
            }

            ext.Append(kv.Key).Append('=').Append(kv.Value);
        }

        return "RemotingCommand [code=" + Code.ToString(CultureInfo.InvariantCulture) +
               ", language=" + Language.ToString(CultureInfo.InvariantCulture) +
               ", version=" + Version.ToString(CultureInfo.InvariantCulture) +
               ", opaque=" + Opaque.ToString(CultureInfo.InvariantCulture) +
               ", flag=" + Flag.ToString(CultureInfo.InvariantCulture) +
               ", remark=" + Remark +
               ", extFields={" + ext + "}" +
               ", serializeTypeCurrentRPC=" +
               SerializeTypeCurrentRpc.ToString(CultureInfo.InvariantCulture) + "]";
    }

    public static int NextOpaque() => Interlocked.Increment(ref _opaqueCounter) - 1;

    // ---------------- 私有配置 ----------------

    private static byte LoadSerializeTypeConfig()
    {
        string? v = Environment.GetEnvironmentVariable("ROCKETMQ_SERIALIZE_TYPE")
                    ?? Environment.GetEnvironmentVariable("rocketmq.serialize.type");
        if (v is null)
        {
            return SerializeType.Json;
        }

        return v.Trim().ToUpperInvariant() == "ROCKETMQ"
            ? SerializeType.Rocketmq
            : SerializeType.Json;
    }

    private static int LoadVersionConfig()
    {
        string? v = Environment.GetEnvironmentVariable("rocketmq.remoting.version");
        if (v is null)
        {
            return CurrentVersion;
        }

        return int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n)
            ? n
            : CurrentVersion;
    }

    /// <summary>language 名称 -> 码（兼容 5.x 把 language 序列化为枚举名字符串）。未知回落 JAVA。</summary>
    private static byte LanguageNameToCode(string name) =>
        LanguageCode.FromName(name.Trim().ToUpperInvariant()) ?? LanguageCode.Java;
}
