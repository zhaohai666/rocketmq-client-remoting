// Signature content: first MakeCustomHeaderToNet(), then take the **all** values of
// ExtFields in key order (excluding the Signature key itself; only values are concatenated,
// without keys, without `=`/`&` or other separators), and finally append the raw Body bytes.
// Signature value: standard Base64( HMAC-SHA1(key = secretKey, data = the above content) ).
//
// The crypto primitive comes straight from the BCL (System.Security.Cryptography.HMACSHA1),
// **with no NuGet dependency**.
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Remoting;

public interface IRpcHook
{
    /// <summary>Called before sending. Implementations must ensure their own thread safety; the transport layer does not hold a lock when calling this method.</summary>
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
    /// AccessKey / SecurityToken are written first (they participate in the signature), then
    /// the signature is computed, and finally Signature is written.
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
                continue; // Signature itself does not participate in the signature (matches the broker side)
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
/// Composite hook that runs its hooks in registration order.
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

/// Each facade goes through here uniformly in Start(), so the order cannot be written
/// backwards.
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
