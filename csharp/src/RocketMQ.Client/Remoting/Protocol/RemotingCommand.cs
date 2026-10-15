// Wire format: totalLength(4) | headerLength (top 8 bits hold the serialize type, 4) | header | body
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

    public const int CurrentVersion = 515;

    private static readonly object SerializeTypeLock = new();
    private static int _opaqueCounter;
    private static byte _serializeTypeConfig = LoadSerializeTypeConfig();
    private static readonly int VersionConfig = LoadVersionConfig();

    public int Code { get; set; }

    /// <summary>The online language identifier uses DOTNET(2); do not confuse it with the CPP(1) in the wire format.</summary>
    public byte Language { get; set; } = LanguageCode.Dotnet;

    public int Version { get; set; }

    /// <summary>
    /// WARNING: opaque starts counting at 0, so the first request's legal value is 0 —
    /// **0 cannot be used as an "unset" sentinel**. The transport layer only reassigns it when
    /// it truly conflicts with an in-flight request (see RemotingClient).
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

    // ---------------- factory methods ----------------

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

    /// <summary>Whether ROCKETMQ binary serialization is globally used for this RPC.</summary>
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

    // ---------------- frame utilities ----------------

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

    // ---------------- codec ----------------

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

    /// <summary>Encodes only the header (body length counted separately), for framed sending.</summary>
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

    /// <summary>Returns false on parse failure (err is the failure reason); does not throw.</summary>
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
                // 5.x serializes language as an **enum-name string** (4.x used an int); accept both
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

    /// <summary>Throws <see cref="RemotingCommandException"/> on parse failure.</summary>
    public static RemotingCommand Decode(byte[] data)
    {
        if (!TryDecode(data, out RemotingCommand cmd, out string? err))
        {
            throw new RemotingCommandException(string.IsNullOrEmpty(err) ? "decode error" : err);
        }

        return cmd;
    }

    /// <summary>Maps extFields back onto a custom header object.</summary>
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

    // ---------------- private config ----------------

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

    /// <summary>language name -> code (compat: 5.x serializes language as an enum-name string). Falls back to JAVA when unknown.</summary>
    private static byte LanguageNameToCode(string name) =>
        LanguageCode.FromName(name.Trim().ToUpperInvariant()) ?? LanguageCode.Java;
}
