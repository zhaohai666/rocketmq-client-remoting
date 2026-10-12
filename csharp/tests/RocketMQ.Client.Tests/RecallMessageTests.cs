// 定时消息撤回（RecallMessage 370）的离线对拍。
//
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

    private const string LegacyVector =
        "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgMDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=";

    // ---------------------------------------------------------------- 句柄编解码

    [Fact]
    public void BuildHandleMatchesLegacyVector()
    {
        Assert.Equal(LegacyVector,
            RecallMessageHandle.BuildHandle(Topic, Broker, Timestamp, UniqKey));
    }

    [Fact]
    public void DecodeHandleReadsBackEverySegment()
    {
        HandleV1 h = RecallMessageHandle.DecodeHandle(LegacyVector);
        Assert.Equal(Topic, h.Topic);
        Assert.Equal(Broker, h.BrokerName);
        Assert.Equal(Timestamp, h.TimestampStr);
        Assert.Equal(UniqKey, h.MessageId);
    }

    [Fact]
    public void DecodeHandleAcceptsUnpaddedBase64Url()
    {
        string unpadded = LegacyVector.TrimEnd('=');
        Assert.DoesNotContain('=', unpadded);
        HandleV1 h = RecallMessageHandle.DecodeHandle(unpadded);
        Assert.Equal(UniqKey, h.MessageId);
    }

    [Fact]
    public void DecodeHandleIgnoresExtraSegments()
    {
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
    public void InvalidHandlesFailWithLegacyMessage(string handle)
    {
        MQClientException e = Assert.Throws<MQClientException>(
            () => RecallMessageHandle.DecodeHandle(handle));
        Assert.Equal(RecallMessageHandle.InvalidHandle, e.Message);
    }

    // ---------------------------------------------------------------- 协议头字段名

    [Fact]
    public void RecallRequestHeaderUsesLegacyKeys()
    {
        var h = new RecallMessageRequestHeader
        {
            ProducerGroup = "PG_recall",
            Topic = Topic,
            RecallHandle = LegacyVector,
            Bname = Broker,
        };
        PropertyMap ext = h.ToExtFields();

        Assert.Equal(4, ext.Count);
        Assert.Equal("PG_recall", ext["producerGroup"]);
        Assert.Equal(Topic, ext["topic"]);
        Assert.Equal(LegacyVector, ext["recallHandle"]);
        Assert.Equal(Broker, ext["bname"]);
        Assert.False(ext.ContainsKey("brokerName"));

        var back = new RecallMessageRequestHeader();
        back.FromExtFields(ext);
        Assert.Equal(Broker, back.Bname);
        Assert.Equal(LegacyVector, back.RecallHandle);
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
            ["recallHandle"] = LegacyVector,
        };
        var h = new SendMessageResponseHeader();
        h.FromExtFields(ext);
        Assert.Equal(LegacyVector, h.RecallHandle);

        var plain = new SendMessageResponseHeader();
        plain.FromExtFields(new PropertyMap { ["msgId"] = UniqKey });
        Assert.Null(plain.RecallHandle);
    }

    [Fact]
    public void RecallMessageCodeMatchesLegacy()
    {
        Assert.Equal(370, RequestCode.RecallMessage);
    }

    // ---------------------------------------------------------------- 本地校验顺序

    [Fact]
    public void RecallBeforeStartIsRejected()
    {
        var p = new DefaultMQProducer("PG_recall_unit");
        MQClientException e = Assert.Throws<MQClientException>(
            () => p.RecallMessage(Topic, LegacyVector));
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
                RecallMessageThrows(p, "%RETRY%PG_recall_unit", LegacyVector));
            Assert.Equal("topic is not supported",
                RecallMessageThrows(p, "%DLQ%PG_recall_unit", LegacyVector));
            Assert.NotNull(RecallMessageThrows(p, "bad topic!", LegacyVector));

            var began = Stopwatch.GetTimestamp();
            string corrupt = RecallMessageThrows(p, Topic, "not-a-handle") ?? string.Empty;
            double costMs = Stopwatch.GetElapsedTime(began).TotalMilliseconds;
            Assert.Equal(RecallMessageHandle.InvalidHandle, corrupt);
            Assert.True(costMs < 200, "非法句柄必须不打网络，实际 " + costMs + "ms");

            Assert.Equal("Can not find Message Queue for topic: " + Topic,
                RecallMessageThrows(p, Topic, LegacyVector));
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
