//   RemotingCommand cmd = RemotingCommand.createRequestCommand(310, null);
//   cmd.setExtFields(...); cmd.setBody(...);
//   new AclClientRPCHook(new SessionCredentials(ak, sk[, token])).doBeforeRequest(addr, cmd);
//   -> AclUtils.combineRequestContent(...) and cmd.getExtFields().get("Signature")
//
// Any deviation in concatenation order / separator / charset / Base64 alphabet shows up immediately.
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class AclTests
{
    private const string SecretKey = "SK_TEST_SECRET_12345678";

    private static RemotingCommand MakeRequest(
        (string Key, string Value)[] ext, byte[]? body = null)
    {
        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(310, null);
        foreach ((string key, string value) in ext)
        {
            cmd.AddExtField(key, value);
        }

        if (body is { Length: > 0 })
        {
            cmd.Body = body;
            cmd.HasBody = true;
        }

        return cmd;
    }

    private static (string Key, string Value)[] V1Ext() => new[]
    {
        ("topic", "MyTopic"), ("producerGroup", "MyGroup"), ("a", "1"),
        ("Zz", "last"), ("batch", "false"),
    };

    private static byte[] V1Body() => new byte[] { 1, 2, 3, 4, 5 };

    private static string Hex(byte[] data) => Convert.ToHexString(data).ToLowerInvariant();

    // ---------------------------------------------------------------- V1: multiple fields + body, no token
    [Fact]
    public void V1_ContentAndSignatureMatchLegacy()
    {
        RemotingCommand cmd = MakeRequest(V1Ext(), V1Body());
        new AclClientRPCHook(new SessionCredentials("AK_TEST", SecretKey))
            .DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.Equal("AK_TEST", cmd.GetExtField(SessionCredentials.AccessKeyField));
        Assert.Equal(string.Empty, cmd.GetExtField(SessionCredentials.SecurityTokenField));
        Assert.Equal(
            "414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405",
            Hex(AclClientRPCHook.BuildRequestContent(cmd)));
        Assert.Equal("qQhdzvXfV+g0r8LdwNCt+chJ4XY=", cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- V2: with SecurityToken
    [Fact]
    public void V2_TokenParticipatesInSignature()
    {
        RemotingCommand cmd = MakeRequest(V1Ext(), V1Body());
        new AclClientRPCHook(new SessionCredentials("AK_TEST", SecretKey, "TOKEN-ABC"))
            .DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.Equal("TOKEN-ABC", cmd.GetExtField(SessionCredentials.SecurityTokenField));
        Assert.Equal(
            "414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f75704d79546f7069630102030405",
            Hex(AclClientRPCHook.BuildRequestContent(cmd)));
        Assert.Equal("5YIp2FNQL8pxQP3w6YKnSv3kAsw=", cmd.GetExtField(SessionCredentials.SignatureField));
        Assert.NotEqual("qQhdzvXfV+g0r8LdwNCt+chJ4XY=",
            cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- V3: no body
    [Fact]
    public void V3_WithoutBodyMatchesLegacy()
    {
        RemotingCommand cmd = MakeRequest(new[] { ("a", "1"), ("b", "2") });
        new AclClientRPCHook(new SessionCredentials("AK", "SK"))
            .DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.Equal("414b3132", Hex(AclClientRPCHook.BuildRequestContent(cmd)));
        Assert.Equal("d3vJKL2iRdr4ZykZfY+lxfQlfdc=", cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- Insertion order does not matter
    [Fact]
    public void SignatureIsIndependentOfInsertionOrder()
    {
        RemotingCommand a = MakeRequest(V1Ext(), V1Body());
        RemotingCommand b = MakeRequest(
            new[]
            {
                ("Zz", "last"), ("batch", "false"), ("a", "1"),
                ("producerGroup", "MyGroup"), ("topic", "MyTopic"),
            },
            V1Body());

        var hook = new AclClientRPCHook(new SessionCredentials("AK_TEST", SecretKey));
        hook.DoBeforeRequest("x", a);
        hook.DoBeforeRequest("x", b);
        Assert.Equal(a.GetExtField(SessionCredentials.SignatureField),
            b.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- Signature must be excluded
    [Fact]
    public void PreExistingSignatureFieldIsExcluded()
    {
        RemotingCommand withoutSig = MakeRequest(new[] { ("topic", "MyTopic") });
        RemotingCommand withFakeSig = MakeRequest(
            new[] { ("topic", "MyTopic"), ("Signature", "SHOULD_BE_EXCLUDED") });

        Assert.Equal("MyTopic", Encoding.UTF8.GetString(
            AclClientRPCHook.BuildRequestContent(withFakeSig)));
        Assert.Equal(
            AclClientRPCHook.BuildRequestContent(withoutSig),
            AclClientRPCHook.BuildRequestContent(withFakeSig));

        // both sides must compute the same signature: Signature does not take part in signing itself
        Assert.Equal(
            AclClientRPCHook.CalcSignature("SK", withoutSig),
            AclClientRPCHook.CalcSignature("SK", withFakeSig));
    }

    // ---------------------------------------------------------------- The HMAC-SHA1 primitive (RFC 2202)
    [Fact]
    public void HmacSha1MatchesRfc2202()
    {
        Assert.Equal("b617318655057264e28bc0b6fb378c8ef146be00",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                new string((char)0x0b, 20), Encoding.UTF8.GetBytes("Hi There")))));
        Assert.Equal("effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                "Jefe", Encoding.UTF8.GetBytes("what do ya want for nothing?")))));
        // a key longer than the 64-byte block: it must be compressed with SHA1 to 20 bytes and then used as the key.
        // Note the key is ASCII (80 'k's): HmacSha1Base64's key parameter is a string and is encoded as UTF-8
        // bytes, so a non-ASCII character like (char)0xaa would encode as 2 bytes and the vector would not hold.
        // The expected values are recorded golden vectors that can be independently recomputed with a standard HMAC-SHA1:
        //   HMAC-SHA1(key=b'k'*80, msg=b'Test Using Larger Than Block-Size Key - Hash Key First')
        Assert.Equal("a08bbef5673f2c2a8bc6faf91fb5fb9b5ef7da35",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                new string('k', 80),
                Encoding.UTF8.GetBytes("Test Using Larger Than Block-Size Key - Hash Key First")))));
    }

    // ---------------------------------------------------------------- Standard Base64 (RFC 4648)
    [Fact]
    public void Base64IsStandardAlphabet()
    {
        Assert.Equal("TWFu", Convert.ToBase64String(Encoding.UTF8.GetBytes("Man")));
        Assert.Equal("TQ==", Convert.ToBase64String(Encoding.UTF8.GetBytes("M")));
        // the standard alphabet contains '+' '/' (URL-safe substitutes -_)
        Assert.Equal("+/+/", Convert.ToBase64String(new byte[] { 0xfb, 0xff, 0xbf }));
    }

    // ---------------------------------------------------------------- No hook registered = zero changes
    [Fact]
    public void NoHookMeansNoMutation()
    {
        RemotingCommand cmd = MakeRequest(new[] { ("topic", "MyTopic") }, Encoding.UTF8.GetBytes("body"));
        Assert.Single(cmd.ExtFields);
        Assert.Equal(string.Empty, cmd.GetExtField(SessionCredentials.AccessKeyField));
        Assert.Equal(string.Empty, cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- Hook registration semantics: first-wins
    [Fact]
    public void RegisterRpcHookIsFirstWins()
    {
        using var client = new RemotingClient();
        var first = new AclClientRPCHook(new SessionCredentials("AK1", "SK1"));
        var second = new AclClientRPCHook(new SessionCredentials("AK2", "SK2"));

        Assert.True(client.RegisterRpcHook(first));
        Assert.False(client.RegisterRpcHook(second)); // a hook already exists -> not overwritten
        client.UnregisterRpcHook();
        Assert.True(client.RegisterRpcHook(second)); // it can be registered again after unregistering
    }

    // ------------------------------------------------- The combination order of stream + ACL (ReqT must be inside the signature)

    /// The transport layer has only one slot, and the order is restored by
    /// RequestHooks.Compose; the shape it returns is pinned here.
    [Fact]
    public void ComposeFollowsLegacyHookOrder()
    {
        var acl = new AclClientRPCHook(new SessionCredentials("AK", "SK"));

        Assert.Null(RequestHooks.Compose(false, null));
        Assert.Same(acl, RequestHooks.Compose(false, acl));
        Assert.IsType<StreamTypeRPCHook>(RequestHooks.Compose(true, null));

        var chain = Assert.IsType<ChainedRpcHook>(RequestHooks.Compose(true, acl));
        Assert.IsType<StreamTypeRPCHook>(chain.Hooks[0]); // stream comes first
        Assert.Same(acl, chain.Hooks[1]); // the user hook (the signature) comes after
    }

    /// With stream on, ReqT must land inside the signed content: recomputing once from the
    /// post-signature ExtFields and getting a match means the broker recomputing it the
    /// same way will match too.
    [Fact]
    public void ReqTIsSignedWhenStreamTypeHookRunsFirst()
    {
        var acl = new AclClientRPCHook(new SessionCredentials("AK_TEST", SecretKey));
        IRpcHook composed = RequestHooks.Compose(true, acl)!;

        RemotingCommand cmd = MakeRequest(V1Ext(), V1Body());
        composed.DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.Equal("0", cmd.GetExtField(MixAll.ReqT));
        Assert.Equal(
            AclClientRPCHook.CalcSignature(SecretKey, cmd),
            cmd.GetExtField(SessionCredentials.SignatureField));
    }

    /// Counter-proof: with the order reversed the same assertions must go red, otherwise
    /// the previous test has no teeth.
    [Fact]
    public void ReversedHookOrderBreaksTheSignature()
    {
        var acl = new AclClientRPCHook(new SessionCredentials("AK_TEST", SecretKey));
        var reversed = new ChainedRpcHook(new IRpcHook[] { acl, new StreamTypeRPCHook() });

        RemotingCommand cmd = MakeRequest(V1Ext(), V1Body());
        reversed.DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.NotEqual(
            AclClientRPCHook.CalcSignature(SecretKey, cmd),
            cmd.GetExtField(SessionCredentials.SignatureField));
    }

    [Fact]
    public void ChainedHookForwardsAfterResponse()
    {
        var counting = new CountingHook();
        var chain = new ChainedRpcHook(new IRpcHook[] { new StreamTypeRPCHook(), counting });
        RemotingCommand cmd = MakeRequest(new[] { ("topic", "MyTopic") });

        chain.DoAfterResponse("addr", cmd, null);
        Assert.Equal(1, counting.After);
        chain.DoBeforeRequest("addr", cmd);
        Assert.Equal(1, counting.Before);
    }

    private sealed class CountingHook : IRpcHook
    {
        public int Before { get; private set; }

        public int After { get; private set; }

        public void DoBeforeRequest(string remoteAddr, RemotingCommand request) => Before++;

        public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
            => After++;
    }
}
