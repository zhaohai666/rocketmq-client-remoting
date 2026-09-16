// ACL 鉴权单测：AclClientRPCHook 的签名内容与签名值**逐字节对拍 Java 官方实现**。
//
// 下面各向量的 content / signature 全部由 Java 官方
// org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest 跑出来：
//
//   RemotingCommand cmd = RemotingCommand.createRequestCommand(310, null);
//   cmd.setExtFields(...); cmd.setBody(...);
//   new AclClientRPCHook(new SessionCredentials(ak, sk[, token])).doBeforeRequest(addr, cmd);
//   -> AclUtils.combineRequestContent(...) 与 cmd.getExtFields().get("Signature")
//
// 任何一处拼接顺序 / 分隔符 / 字符集 / Base64 字母表的偏差都会立刻暴露。
using System.Text;
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

    // ---------------------------------------------------------------- V1: 多字段 + body，无 token
    [Fact]
    public void V1_ContentAndSignatureMatchJava()
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

    // ---------------------------------------------------------------- V2: 带 SecurityToken
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

    // ---------------------------------------------------------------- V3: 无 body
    [Fact]
    public void V3_WithoutBodyMatchesJava()
    {
        RemotingCommand cmd = MakeRequest(new[] { ("a", "1"), ("b", "2") });
        new AclClientRPCHook(new SessionCredentials("AK", "SK"))
            .DoBeforeRequest("127.0.0.1:9876", cmd);

        Assert.Equal("414b3132", Hex(AclClientRPCHook.BuildRequestContent(cmd)));
        Assert.Equal("d3vJKL2iRdr4ZykZfY+lxfQlfdc=", cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- 插入顺序无关
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

    // ---------------------------------------------------------------- Signature 必须被排除
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

        // 两者算出的签名必须一致：Signature 不参与自己的签名
        Assert.Equal(
            AclClientRPCHook.CalcSignature("SK", withoutSig),
            AclClientRPCHook.CalcSignature("SK", withFakeSig));
    }

    // ---------------------------------------------------------------- HMAC-SHA1 原语（RFC 2202）
    [Fact]
    public void HmacSha1MatchesRfc2202()
    {
        Assert.Equal("b617318655057264e28bc0b6fb378c8ef146be00",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                new string((char)0x0b, 20), Encoding.UTF8.GetBytes("Hi There")))));
        Assert.Equal("effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                "Jefe", Encoding.UTF8.GetBytes("what do ya want for nothing?")))));
        // key 长于 64 字节块：必须先 SHA1 压缩成 20 字节再当 key。
        // 注意 key 用 ASCII（80 个 'k'）：HmacSha1Base64 的 key 形参是 string，会按 UTF-8
        // 编码成字节，用 (char)0xaa 这种非 ASCII 字符会编成 2 字节，向量就不成立了。
        // 期望值由 Python hmac/hashlib（独立实现）算出：
        //   hmac.new(b'k'*80, b'Test Using Larger Than Block-Size Key - Hash Key First', sha1)
        Assert.Equal("a08bbef5673f2c2a8bc6faf91fb5fb9b5ef7da35",
            Hex(Convert.FromBase64String(AclClientRPCHook.HmacSha1Base64(
                new string('k', 80),
                Encoding.UTF8.GetBytes("Test Using Larger Than Block-Size Key - Hash Key First")))));
    }

    // ---------------------------------------------------------------- 标准 Base64（RFC 4648）
    [Fact]
    public void Base64IsStandardAlphabet()
    {
        Assert.Equal("TWFu", Convert.ToBase64String(Encoding.UTF8.GetBytes("Man")));
        Assert.Equal("TQ==", Convert.ToBase64String(Encoding.UTF8.GetBytes("M")));
        // 标准字母表含 '+' '/'（URL-safe 会用 -_ 代替）
        Assert.Equal("+/+/", Convert.ToBase64String(new byte[] { 0xfb, 0xff, 0xbf }));
    }

    // ---------------------------------------------------------------- 未注册钩子 = 零改动
    [Fact]
    public void NoHookMeansNoMutation()
    {
        RemotingCommand cmd = MakeRequest(new[] { ("topic", "MyTopic") }, Encoding.UTF8.GetBytes("body"));
        Assert.Single(cmd.ExtFields);
        Assert.Equal(string.Empty, cmd.GetExtField(SessionCredentials.AccessKeyField));
        Assert.Equal(string.Empty, cmd.GetExtField(SessionCredentials.SignatureField));
    }

    // ---------------------------------------------------------------- 钩子注册语义：first-wins
    [Fact]
    public void RegisterRpcHookIsFirstWins()
    {
        using var client = new RemotingClient();
        var first = new AclClientRPCHook(new SessionCredentials("AK1", "SK1"));
        var second = new AclClientRPCHook(new SessionCredentials("AK2", "SK2"));

        Assert.True(client.RegisterRpcHook(first));
        Assert.False(client.RegisterRpcHook(second)); // 已有钩子 → 不覆盖
        client.UnregisterRpcHook();
        Assert.True(client.RegisterRpcHook(second)); // 注销后可再注册
    }
}
