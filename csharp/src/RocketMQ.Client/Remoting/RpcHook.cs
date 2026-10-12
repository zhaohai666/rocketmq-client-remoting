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

public interface IRpcHook
{
    /// <summary>发送前调用。实现必须自行保证线程安全；传输层不持锁调用本方法。</summary>
    void DoBeforeRequest(string remoteAddr, RemotingCommand request);

    void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response);
}

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
        _ = remoteAddr;
        _ = request;
        _ = response;
    }

    public static byte[] BuildRequestContent(RemotingCommand request)
    {
        request.MakeCustomHeaderToNet();

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

    public static string CalcSignature(string secretKey, RemotingCommand request)
    {
        byte[] data = BuildRequestContent(request);
        return HmacSha1Base64(secretKey, data);
    }

    public static string HmacSha1Base64(string key, byte[] data)
    {
        using var mac = new HMACSHA1(Encoding.UTF8.GetBytes(key));
        return Convert.ToBase64String(mac.ComputeHash(data));
    }
}

public sealed class StreamTypeRPCHook : IRpcHook
{
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

public sealed class NamespaceRpcHook : IRpcHook
{
    private readonly Func<string?> _namespaceV2;

    public NamespaceRpcHook(Func<string?> namespaceV2)
    {
        _namespaceV2 = namespaceV2;
    }

    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
        _ = remoteAddr;
        string ns = _namespaceV2() ?? string.Empty;
        if (ns.Length == 0)
        {
            return;
        }

        request.AddExtField(MixAll.RpcNamespacedField, "true");
        request.AddExtField(MixAll.RpcNamespaceField, ns);
    }

    public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
    {
        _ = remoteAddr;
        _ = request;
        _ = response;
    }
}

/// 各 facade 在 Start() 里统一走这里，顺序就不会写反。
/// </summary>
public static class RequestHooks
{
    public static IRpcHook? Compose(bool enableStreamRequestType, IRpcHook? userHook,
        Func<string?>? namespaceV2Getter = null)
    {
        bool hasNamespace = namespaceV2Getter is not null;
        if (!hasNamespace && !enableStreamRequestType)
        {
            return userHook;
        }

        var hooks = new List<IRpcHook>();
        if (namespaceV2Getter is not null)
        {
            hooks.Add(new NamespaceRpcHook(namespaceV2Getter));
        }

        if (enableStreamRequestType)
        {
            hooks.Add(new StreamTypeRPCHook());
        }

        if (userHook is not null)
        {
            hooks.Add(userHook);
        }

        return hooks.Count == 1 ? hooks[0] : new ChainedRpcHook(hooks);
    }
}
