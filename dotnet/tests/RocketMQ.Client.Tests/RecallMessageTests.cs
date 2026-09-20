// 定时消息撤回（RecallMessage 370）的离线对拍。
//
// 覆盖三层：句柄编解码（Java RecallMessageHandle 的 vector）、协议头字段名（broker 用
// fastjson2 按属性名反序列化，写错就是静默丢字段）、producer 侧的本地校验顺序。
// 真正打到 broker 的端到端验证在 examples/RocketMQ.Examples/Program.cs 的 live_recall 里，
// 因为撤回开关（recallMessageEnable）默认关着，不能塞进单测。
//
// 与 python/tests/test_recall_message.py、cpp/tests/test_recall_message.cpp、
// rust/src/common/recall_message_handle.rs 的测试同题。
using System.Diagnostics;
using RocketMQ.Remoting.Protocol;
using Xunit;
// 与 src 侧的 global using 保持一致（测试项目没有该全局别名）
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class RecallMessageTests
{
    private const string Topic = "TopicA";
    private const string Broker = "broker-a";
    private const string UniqKey = "0123456789ABCDEF0123456789abcdef";
    private const string Timestamp = "1700000000000";

    /// <summary>Java RecallMessageHandle.buildHandle 的实际输出（带 '=' 填充）。</summary>
    private const string JavaVector =
        "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgMDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=";

    // ---------------------------------------------------------------- 句柄编解码

    [Fact]
    public void BuildHandleMatchesJavaVector()
    {
        Assert.Equal(JavaVector,
            RecallMessageHandle.BuildHandle(Topic, Broker, Timestamp, UniqKey));
    }

    [Fact]
    public void DecodeHandleReadsBackEverySegment()
    {
        HandleV1 h = RecallMessageHandle.DecodeHandle(JavaVector);
        Assert.Equal(Topic, h.Topic);
        Assert.Equal(Broker, h.BrokerName);
        Assert.Equal(Timestamp, h.TimestampStr);
        Assert.Equal(UniqKey, h.MessageId);
    }

    [Fact]
    public void DecodeHandleAcceptsUnpaddedBase64Url()
    {
        // Java 的 getUrlDecoder 只吃带填充的串；本端口四个移植版都放宽成两种通吃，
        // 否则别的客户端写下的无填充句柄就撤不回。
        string unpadded = JavaVector.TrimEnd('=');
        Assert.DoesNotContain('=', unpadded);
        HandleV1 h = RecallMessageHandle.DecodeHandle(unpadded);
        Assert.Equal(UniqKey, h.MessageId);
    }

    [Fact]
    public void DecodeHandleIgnoresExtraSegments()
    {
        // Java split 后只取 items[1..4]，尾段（新版句柄格式）忽略而不是报错。
        HandleV1 h = RecallMessageHandle.DecodeHandle(
            "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgYWJjIGp1bms=");
        Assert.Equal("abc", h.MessageId);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not-a-handle")]
    [InlineData("!!!!")]
    // v2：版本段不认识就拒，避免把新格式当成 v1 解出错位字段
    [InlineData("djIgVG9waWNBIGIgMSBpZA==")]
    // 只有 4 段：缺 messageId
    [InlineData("djEgVG9waWNBIGIgMQ==")]
    // 合法 base64、非法 utf-8
    [InlineData("__4gYmFkIHV0Zjg=")]
    public void InvalidHandlesFailWithJavaMessage(string handle)
    {
        MQClientException e = Assert.Throws<MQClientException>(
            () => RecallMessageHandle.DecodeHandle(handle));
        Assert.Equal(RecallMessageHandle.InvalidHandle, e.Message);
    }

    // ---------------------------------------------------------------- 协议头字段名

    [Fact]
    public void RecallRequestHeaderUsesJavaKeys()
    {
        var h = new RecallMessageRequestHeader
        {
            ProducerGroup = "PG_recall",
            Topic = Topic,
            RecallHandle = JavaVector,
            Bname = Broker,
        };
        PropertyMap ext = h.ToExtFields();

        Assert.Equal(4, ext.Count);
        Assert.Equal("PG_recall", ext["producerGroup"]);
        Assert.Equal(Topic, ext["topic"]);
        Assert.Equal(JavaVector, ext["recallHandle"]);
        // 继承字段在 Java 里反射名就是 bname，写成 brokerName 会被 broker 静默丢掉
        Assert.Equal(Broker, ext["bname"]);
        Assert.False(ext.ContainsKey("brokerName"));

        var back = new RecallMessageRequestHeader();
        back.FromExtFields(ext);
        Assert.Equal(Broker, back.Bname);
        Assert.Equal(JavaVector, back.RecallHandle);
    }

    [Fact]
    public void RecallResponseHeaderCarriesOnlyMsgId()
    {
        var ext = new PropertyMap { ["msgId"] = UniqKey };
        var h = new RecallMessageResponseHeader();
        h.FromExtFields(ext);
        Assert.Equal(UniqKey, h.MsgId);
        Assert.Equal(UniqKey, h.ToExtFields()["msgId"]);
    }

    [Fact]
    public void SendResponseHeaderParsesRecallHandle()
    {
        // broker 只对定时消息挂 recallHandle；普通消息不带这个键，解出来必须是 null。
        var ext = new PropertyMap
        {
            ["msgId"] = UniqKey,
            ["queueId"] = "0",
            ["queueOffset"] = "42",
            ["transactionId"] = "tx-1",
            ["recallHandle"] = JavaVector,
        };
        var h = new SendMessageResponseHeader();
        h.FromExtFields(ext);
        Assert.Equal(JavaVector, h.RecallHandle);

        var plain = new SendMessageResponseHeader();
        plain.FromExtFields(new PropertyMap { ["msgId"] = UniqKey });
        Assert.Null(plain.RecallHandle);
    }

    [Fact]
    public void RecallMessageCodeMatchesJava()
    {
        Assert.Equal(370, RequestCode.RecallMessage);
    }

    // ---------------------------------------------------------------- 本地校验顺序

    [Fact]
    public void RecallBeforeStartIsRejected()
    {
        var p = new DefaultMQProducer("PG_recall_unit");
        MQClientException e = Assert.Throws<MQClientException>(
            () => p.RecallMessage(Topic, JavaVector));
        Assert.Equal("producer not started, call start() first", e.Message);
    }

    [Fact]
    public void LocalValidationRunsBeforeAnyNetworkCall()
    {
        // 名字服务器指向必然被拒绝的端口：本地校验若在打网络之前跑完，就必须秒回，
        // 且抛的是**校验**错误而不是 RPC 超时。
        var p = new DefaultMQProducer("PG_recall_unit") { NamesrvAddr = "127.0.0.1:1" };
        p.Start();
        try
        {
            Assert.Equal("topic is not supported",
                RecallMessageThrows(p, "%RETRY%PG_recall_unit", JavaVector));
            Assert.Equal("topic is not supported",
                RecallMessageThrows(p, "%DLQ%PG_recall_unit", JavaVector));
            Assert.NotNull(RecallMessageThrows(p, "bad topic!", JavaVector));

            var began = Stopwatch.GetTimestamp();
            string corrupt = RecallMessageThrows(p, Topic, "not-a-handle") ?? string.Empty;
            double costMs = Stopwatch.GetElapsedTime(began).TotalMilliseconds;
            Assert.Equal(RecallMessageHandle.InvalidHandle, corrupt);
            Assert.True(costMs < 200, "非法句柄必须不打网络，实际 " + costMs + "ms");

            // 句柄合法但拿不到路由：Java 的 tryToFindTopicPublishInfo 异常照抛
            // （DefaultMQProducerImpl:1586），所以这里 surfacing 的是路由错误而不是
            // "The broker service address not found"。
            Assert.Equal("Can not find Message Queue for topic: " + Topic,
                RecallMessageThrows(p, Topic, JavaVector));
        }
        finally
        {
            p.Shutdown();
        }
    }

    private static string? RecallMessageThrows(DefaultMQProducer producer, string topic,
        string handle)
    {
        try
        {
            _ = producer.RecallMessage(topic, handle);
            return null;
        }
        catch (MQClientException e)
        {
            return e.Message;
        }
    }
}
