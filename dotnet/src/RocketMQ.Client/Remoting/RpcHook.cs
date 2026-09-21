// RPC 钩子（对应 org.apache.rocketmq.remoting.RPCHook）。
//
// AclClientRPCHook：基于 accessKey/secretKey 计算签名并注入 ExtFields
// （AccessKey / Signature / SecurityToken）。签名算法**逐字节对齐** Java：
//
//   - org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest
//   - org.apache.rocketmq.acl.common.AclUtils#combineRequestContent
//   - org.apache.rocketmq.acl.common.AclSigner#calSignature
//
// 签名内容：先 MakeCustomHeaderToNet()，再按 key 序取 ExtFields 的**全部** value
// （排除 Signature 键自身；拼接时只有 value，不带 key、不带 `=`/`&` 等分隔符），
// 最后拼上 Body 原始字节。
// 签名值：标准 Base64( HMAC-SHA1(key = secretKey, data = 上述内容) )。
//
// 加密原语直接用 BCL（System.Security.Cryptography.HMACSHA1），**不引入任何 NuGet**。
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Remoting;

/// <summary>对应 org.apache.rocketmq.remoting.RPCHook。</summary>
public interface IRpcHook
{
    /// <summary>发送前调用。实现必须自行保证线程安全；传输层不持锁调用本方法。</summary>
    void DoBeforeRequest(string remoteAddr, RemotingCommand request);

    /// <summary>收到响应后调用（AclClientRPCHook 里是空实现，与 Java 一致）。</summary>
    void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response);
}

/// <summary>对应 org.apache.rocketmq.acl.common.SessionCredentials。</summary>
public sealed class SessionCredentials
{
    public const string AccessKeyField = "AccessKey";
    public const string SecretKeyField = "SecretKey";
    public const string SignatureField = "Signature";
    public const string SecurityTokenField = "SecurityToken";

    public SessionCredentials(string accessKey, string secretKey, string securityToken = "")
    {
        AccessKey = accessKey;
        SecretKey = secretKey;
        SecurityToken = securityToken;
    }

    public string AccessKey { get; }

    public string SecretKey { get; }

    public string SecurityToken { get; }
}

/// <summary>对应 org.apache.rocketmq.acl.common.AclClientRPCHook。</summary>
public sealed class AclClientRPCHook : IRpcHook
{
    private readonly SessionCredentials _credentials;

    public AclClientRPCHook(SessionCredentials credentials)
    {
        _credentials = credentials;
    }

    public SessionCredentials Credentials => _credentials;

    /// <summary>
    /// AccessKey / SecurityToken 先写入（它们参与签名），再算签名，最后写 Signature。
    /// </summary>
    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
        _ = remoteAddr;
        request.AddExtField(SessionCredentials.AccessKeyField, _credentials.AccessKey);
        if (_credentials.SecurityToken.Length > 0)
        {
            request.AddExtField(SessionCredentials.SecurityTokenField, _credentials.SecurityToken);
        }

        string signature = CalcSignature(_credentials.SecretKey, request);
        request.AddExtField(SessionCredentials.SignatureField, signature);
    }

    public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
    {
        // 与 Java 一致：空实现
        _ = remoteAddr;
        _ = request;
        _ = response;
    }

    /// <summary>
    /// 对应 Java AclUtils.combineRequestContent：内部先 MakeCustomHeaderToNet()，
    /// 再按 key 序拼接全部 value（跳过 Signature），最后拼 Body。
    /// </summary>
    public static byte[] BuildRequestContent(RemotingCommand request)
    {
        // customHeader 的字段同样是签名内容的一部分（对应 Java parseRequestContent）。
        request.MakeCustomHeaderToNet();

        // 显式用 Ordinal 排序：Java 的 TreeMap 按 String 自然序（UTF-16 码元）比较，
        // 与 Ordinal 等价；这里不依赖 SortedDictionary 的默认比较器（它是 culture-sensitive 的）。
        var keys = new List<string>(request.ExtFields.Keys);
        keys.Sort(StringComparer.Ordinal);

        using var ms = new MemoryStream();
        foreach (string key in keys)
        {
            if (key == SessionCredentials.SignatureField)
            {
                continue; // Signature 自身不参与签名（与 broker 侧一致）
            }

            byte[] value = Encoding.UTF8.GetBytes(request.ExtFields[key]);
            ms.Write(value, 0, value.Length);
        }

        if (request.HasBody && request.Body.Length > 0)
        {
            ms.Write(request.Body, 0, request.Body.Length);
        }

        return ms.ToArray();
    }

    /// <summary>对应 Java AclSigner.calSignature：HmacSHA1 + 标准 Base64（带 '=' 填充）。</summary>
    public static string CalcSignature(string secretKey, RemotingCommand request)
    {
        byte[] data = BuildRequestContent(request);
        return HmacSha1Base64(secretKey, data);
    }

    /// <summary>HMAC-SHA1 + 标准 Base64。对应 Java AclSigner.calSignature。</summary>
    public static string HmacSha1Base64(string key, byte[] data)
    {
        using var mac = new HMACSHA1(Encoding.UTF8.GetBytes(key));
        return Convert.ToBase64String(mac.ComputeHash(data));
    }
}

/// <summary>
/// 对应 org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook：
/// 给每笔请求打 <c>ReqT = String.valueOf(RequestType.STREAM.getCode())</c>，即字面量
/// <c>"0"</c>（clientId 的后缀才用枚举名 <c>@STREAM</c>，两处口径不同别混）。
/// 开关是 <c>ClientConfig#enableStreamRequestType</c>，Java 只有 pull / lite 消费者
/// 在构造函数里置真（DefaultMQPullConsumer:113/126、DefaultLitePullConsumer:213/228）。
/// </summary>
public sealed class StreamTypeRPCHook : IRpcHook
{
    /// <summary>对应 Java RequestType.STREAM.getCode()。</summary>
    public const int StreamRequestTypeCode = 0;

    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
        _ = remoteAddr;
        request.AddExtField(MixAll.ReqT,
            StreamRequestTypeCode.ToString(CultureInfo.InvariantCulture));
    }

    public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
    {
        _ = remoteAddr;
        _ = request;
        _ = response;
    }
}

/// <summary>
/// 按注册顺序依次执行的组合钩子。
///
/// Java 的传输层持有 RPCHook **列表**（<c>NettyRemotingAbstract#rpcHooks</c>，按注册顺序
/// 执行），本端口的 RemotingClient 只有一槽（first-wins），所以顺序靠组合还原。
/// 顺序在这里是语义而不是风格：Java <c>MQClientAPIImpl:329-332</c> 的注册顺序是
/// Namespace → <b>Stream</b> → 用户钩子（ACL）→ DynamicalExtField，注释原文
/// "Inject stream rpc hook first to make reserve field signature" —— <c>ReqT</c> 必须在
/// 签名**之前**写入，否则签的内容与真正上线的 ExtFields 不一致，开鉴权的 broker 验签必失败。
/// </summary>
public sealed class ChainedRpcHook : IRpcHook
{
    private readonly IReadOnlyList<IRpcHook> _hooks;

    public ChainedRpcHook(IReadOnlyList<IRpcHook> hooks) => _hooks = hooks;

    public IReadOnlyList<IRpcHook> Hooks => _hooks;

    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
        foreach (IRpcHook hook in _hooks) hook.DoBeforeRequest(remoteAddr, request);
    }

    public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
    {
        foreach (IRpcHook hook in _hooks) hook.DoAfterResponse(remoteAddr, request, response);
    }
}

/// <summary>
/// 按 Java <c>MQClientAPIImpl:329-332</c> 的顺序装好请求钩子：StreamTypeRPCHook 在前、
/// 用户钩子（ACL 签名）在后；只开了 stream 或只有用户钩子时直接返回那一个，
/// 两者都没有时返回 null（不注册钩子 = 零开销）。
///
/// 各 facade 在 Start() 里统一走这里，顺序就不会写反。
/// </summary>
public static class RequestHooks
{
    public static IRpcHook? Compose(bool enableStreamRequestType, IRpcHook? userHook)
    {
        if (!enableStreamRequestType) return userHook;
        if (userHook is null) return new StreamTypeRPCHook();
        return new ChainedRpcHook(new IRpcHook[] { new StreamTypeRPCHook(), userHook });
    }
}
