// Offline unit tests for scheduled message recall (RecallMessage 370): codec, wire key
// names and the local validation order.
using System.Diagnostics;
using RocketMQ.Remoting.Protocol;
using Xunit;
// Kept consistent with the global usings on the src side (the test project does not have that global alias)
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

    // ---------------------------------------------------------------- Handle codec

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
    // v2: an unrecognised version segment is rejected, so a new format is not decoded as a v1 with misaligned fields
    [InlineData("djIgVG9waWNBIGIgMSBpZA==")]
    // only 4 segments: messageId is missing
    [InlineData("djEgVG9waWNBIGIgMQ==")]
    // legal base64, invalid utf-8
    [InlineData("__4gYmFkIHV0Zjg=")]
    public void InvalidHandlesFailWithLegacyMessage(string handle)
    {
        MQClientException e = Assert.Throws<MQClientException>(
            () => RecallMessageHandle.DecodeHandle(handle));
        Assert.Equal(RecallMessageHandle.InvalidHandle, e.Message);
    }

    // ---------------------------------------------------------------- Protocol header field names

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
        // The broker only attaches a recallHandle to scheduled messages; a normal message does not carry that key and must decode to null.
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

    // ---------------------------------------------------------------- Local validation order

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
        // the name server points at a port that is guaranteed to be refused: if the local validation runs before any network access it must come back instantly,
        // and what it throws must be a **validation** error rather than an RPC timeout.
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
            Assert.True(costMs < 200, "an illegal handle must not touch the network, actually " + costMs + "ms");

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
