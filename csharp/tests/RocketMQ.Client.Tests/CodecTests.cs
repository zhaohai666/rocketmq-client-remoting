// Codec round-trip unit tests: JSON / RocketMQ binary / RemotingCommand / message format / utility functions.
using System.Buffers.Binary;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class CodecTests
{
    private static int GetInt32At(byte[] buf, int offset) =>
        BinaryPrimitives.ReadInt32BigEndian(buf.AsSpan(offset, 4));

    // ---------------------------------------------------------------- JSON

    [Fact]
    public void Json_ParseBasic()
    {
        Assert.True(Json.TryParse("{\"a\":1,\"b\":\"x\",\"c\":[1,2,3],\"d\":true,\"e\":null}",
            out JsonValue v, out _));
        Assert.Equal(1, v.Get("a").IntValue());
        Assert.Equal("x", v.Get("b").StringValue());
        Assert.Equal(3, v.Get("c").Size());
        Assert.Equal(3, v.Get("c").At(2).IntValue());
        Assert.True(v.Get("d").BoolValue());
        Assert.True(v.Get("e").IsNull);
    }

    [Fact]
    public void Json_TolerantNumericKey()
    {
        // FastJSON compatibility with bare numeric keys (fastjson2 emits bare numeric keys)
        Assert.True(Json.TryParse("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"}}", out JsonValue v2, out _));
        JsonValue? ba = v2.Find("brokerAddrs");
        Assert.NotNull(ba);
        Assert.Equal("127.0.0.1:10911", ba!.Get("0").StringValue());
    }

    [Fact]
    public void Json_EscapeAndUtf8()
    {
        Assert.True(Json.TryParse("{\"s\":\"a\\nb\\u4e2d\"}", out JsonValue v3, out _));
        Assert.Equal("a\nb中", v3.Get("s").StringValue());
    }

    [Fact]
    public void Json_DumpParseRoundTrip()
    {
        Assert.True(Json.TryParse("{\"a\":1,\"b\":\"x\",\"c\":[1,2,3],\"d\":true,\"e\":null}",
            out JsonValue v, out _));
        Assert.True(Json.TryParse(v.Dump(), out JsonValue v4, out _));
        Assert.Equal(1, v4.Get("a").IntValue());
        Assert.Equal(3, v4.Get("c").Size());
    }

    // ------------------------------------------------- RocketMQ binary serialisation

    [Fact]
    public void WriteDecimalLong_Format()
    {
        // writeDecimalLong: [4B len][ascii]
        var buf = new ByteWriter();
        RocketMQSerializable.WriteDecimalLong(buf, -12345);
        byte[] bytes = buf.ToArray();
        Assert.Equal(6, GetInt32At(bytes, 0));
        Assert.Equal("-12345", Encoding.ASCII.GetString(bytes, 4, bytes.Length - 4));
    }

    [Fact]
    public void WriteStr_ReadStr_RoundTrip()
    {
        // writeStr short/long length
        var s = new ByteWriter();
        RocketMQSerializable.WriteStr(s, true, "abc");
        byte[] bytes = s.ToArray();
        Assert.Equal(2 + 3, bytes.Length);
        Assert.Equal(3, bytes[1]);

        Assert.True(RocketMQSerializable.ReadStr(bytes, 0, true, out string? @out, out int newOffset));
        Assert.Equal("abc", @out);
        Assert.Equal(bytes.Length, newOffset);
    }

    // ------------------------------------------------------ RemotingCommand(JSON)

    [Fact]
    public void RemotingCommand_JsonRoundTrip()
    {
        var hdr = new SendMessageRequestHeader
        {
            ProducerGroup = "pg",
            Topic = "T_TEST",
            QueueId = 2,
            BornTimestamp = 1699999999000L,
        };

        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(RequestCode.SendMessage, hdr);
        cmd.Remark = "hello";
        cmd.HasRemark = true;
        cmd.Body = Encoding.UTF8.GetBytes("payload");

        byte[] wire = cmd.Encode();
        // totalLength = 4(headerLen field) + header + body
        int total = GetInt32At(wire, 0);
        Assert.Equal(wire.Length, total + 4);

        RemotingCommand dec = RemotingCommand.Decode(wire);
        Assert.Equal(RequestCode.SendMessage, dec.Code);
        Assert.Equal("hello", dec.Remark);
        Assert.Equal("payload", Encoding.UTF8.GetString(dec.Body));
        Assert.Equal("T_TEST", dec.GetExtField("topic"));
        Assert.Equal("2", dec.GetExtField("queueId"));
        Assert.Equal("1699999999000", dec.GetExtField("bornTimestamp"));
        Assert.Equal(SerializeType.Json, dec.SerializeTypeCurrentRpc);

        // restore the custom header from extFields
        var back = new SendMessageRequestHeader();
        dec.DecodeCommandCustomHeader(back);
        Assert.Equal("T_TEST", back.Topic);
        Assert.Equal(2, back.QueueId);
    }

    // ------------------------------------------------- RemotingCommand(RocketMQ)

    [Fact]
    public void RemotingCommand_RocketMqRoundTrip()
    {
        var hdr = new PullMessageRequestHeader
        {
            ConsumerGroup = "cg",
            Topic = "T_PULL",
            QueueId = 1,
            QueueOffset = 1234567890123L,
        };

        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(RequestCode.PullMessage, hdr);
        cmd.SerializeTypeCurrentRpc = SerializeType.Rocketmq;
        cmd.Remark = "rk";
        cmd.HasRemark = true;
        cmd.Body = Encoding.UTF8.GetBytes("b1");

        byte[] wire = cmd.Encode();
        RemotingCommand dec = RemotingCommand.Decode(wire);
        Assert.Equal(SerializeType.Rocketmq, dec.SerializeTypeCurrentRpc);
        Assert.Equal(RequestCode.PullMessage, dec.Code);
        Assert.Equal("rk", dec.Remark);
        Assert.Equal("b1", Encoding.UTF8.GetString(dec.Body));
        Assert.Equal("1234567890123", dec.GetExtField("queueOffset"));
        Assert.Equal("cg", dec.GetExtField("consumerGroup"));
    }

    // ---------------------------------------------------------------- V1/V2 headers

    [Fact]
    public void SendMessageHeader_V1V2()
    {
        var v1 = new SendMessageRequestHeader
        {
            ProducerGroup = "pg",
            Topic = "T",
            QueueId = 3,
            BornTimestamp = 42,
            Batch = false,
        };

        SendMessageRequestHeaderV2 v2 = SendMessageRequestHeaderV2.FromV1(v1);
        PropertyMap ext = v2.ToExtFields();
        Assert.Equal("pg", ext["a"]);
        Assert.Equal("T", ext["b"]);
        Assert.Equal("3", ext["e"]);
        Assert.Equal("42", ext["g"]);
        Assert.Equal("false", ext["m"]);

        SendMessageRequestHeader back = v2.ToV1();
        Assert.Equal("pg", back.ProducerGroup);
        Assert.Equal(3, back.QueueId);
        Assert.False(back.Batch ?? true);
    }

    // ---------------------------------------------------------------- Message codec

    [Fact]
    public void MessageExt_EncodeDecode()
    {
        var m = new MessageExt
        {
            Topic = "T_MSG",
            Body = Encoding.UTF8.GetBytes("hello-body"),
            Flag = 7,
            QueueId = 2,
            QueueOffset = 100,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            CommitLogOffset = 4096,
            ReconsumeTimes = 0,
        };
        m.Tags = "TagA";
        m.Keys = "K1";
        m.SetUserProperty("city", "Hangzhou");

        byte[] raw = MessageDecoder.EncodeMessageExt(m, false);
        Assert.True(raw.Length >= 4);
        Assert.Equal(raw.Length, GetInt32At(raw, 0));
        Assert.Equal(MessageDecoder.MessageMagicCode, GetInt32At(raw, MessageDecoder.MessageMagicCodePosition));

        Assert.True(MessageDecoder.DecodeMessage(raw, out MessageExt d, true));
        Assert.Equal("T_MSG", d.Topic);
        Assert.Equal("hello-body", Encoding.UTF8.GetString(d.Body));
        Assert.Equal(2, d.QueueId);
        Assert.Equal(100, d.QueueOffset);
        Assert.Equal(7, d.Flag);
        Assert.Equal("TagA", d.Tags);
        Assert.Equal("K1", d.Keys);
        Assert.Equal("Hangzhou", d.GetUserProperty("city"));
        Assert.NotEmpty(d.MsgId);

        // a multi-message stream
        var stream = new byte[raw.Length * 2];
        Buffer.BlockCopy(raw, 0, stream, 0, raw.Length);
        Buffer.BlockCopy(raw, 0, stream, raw.Length, raw.Length);
        List<MessageExt> list = MessageDecoder.DecodeMessages(stream, true);
        Assert.Equal(2, list.Count);
    }

    // ---------------------------------------------------------------- Batch codec

    [Fact]
    public void BatchCodec_RoundTrip()
    {
        var a = new Message("T_B", Encoding.UTF8.GetBytes("aaa"));
        var b = new Message("T_B", Encoding.UTF8.GetBytes("bbb"));
        b.Tags = "TagB";

        byte[] body = MessageDecoder.EncodeMessages(new List<Message> { a, b });
        Assert.Equal(2, MessageDecoder.CountInnerMsgNum(body));
        List<Message> back = MessageDecoder.DecodeBatchMessages(body);
        Assert.Equal(2, back.Count);
        Assert.Equal("aaa", Encoding.UTF8.GetString(back[0].Body));
        Assert.Equal("TagB", back[1].Tags);

        MessageBatch batch = MessageBatch.GenerateFromList(new List<Message> { a, b });
        Assert.Equal(2, batch.Size);
        Assert.Equal("T_B", batch.Topic);
        Assert.Equal(MessageDecoder.EncodeMessages(new List<Message> { a, b }), batch.Body);
        Assert.Equal(32, MessageClientIDSetter.GetUniqId(a).Length);
        Assert.Equal(32, MessageClientIDSetter.GetUniqId(b).Length);
    }

    [Fact]
    public void BatchDefaultsWaitStoreMsgOkToTrue()
    {
        var a = new Message("T_W", Encoding.UTF8.GetBytes("a"));
        var b = new Message("T_W", Encoding.UTF8.GetBytes("b"));
        Assert.DoesNotContain(MessageConst.PropertyWaitStoreMsgOk, a.Properties.Keys);
        Assert.True(a.WaitStoreMsgOk);

        MessageBatch batch = MessageBatch.GenerateFromList(new List<Message> { a, b });
        Assert.Equal("true", batch.WaitStoreMsgOkStr);
        Assert.True(batch.WaitStoreMsgOk);

        // an explicit false must be carried through as is
        var c = new Message("T_W", Encoding.UTF8.GetBytes("c")) { WaitStoreMsgOk = false };
        var d = new Message("T_W", Encoding.UTF8.GetBytes("d")) { WaitStoreMsgOk = false };
        MessageBatch off = MessageBatch.GenerateFromList(new List<Message> { c, d });
        Assert.Equal("false", off.WaitStoreMsgOkStr);
        Assert.False(off.WaitStoreMsgOk);
    }

    [Fact]
    public void WaitStoreMsgOkFollowsLegacyParseBoolean()
    {
        var msg = new Message("T_W", Encoding.UTF8.GetBytes("x"));
        Assert.True(msg.WaitStoreMsgOk);                                  // the default
        msg.WaitStoreMsgOk = true;
        Assert.True(msg.WaitStoreMsgOk);
        msg.WaitStoreMsgOk = false;
        Assert.False(msg.WaitStoreMsgOk);
        msg.Properties[MessageConst.PropertyWaitStoreMsgOk] = "TRUE";
        Assert.True(msg.WaitStoreMsgOk);
        // every other value is false (not "anything that is not false is true")
        foreach (string raw in new[] { "1", "", "yes", "tru" })
        {
            msg.Properties[MessageConst.PropertyWaitStoreMsgOk] = raw;
            Assert.False(msg.WaitStoreMsgOk);
        }
    }

    // ---------------------------------------------------------------- Utilities and hashing

    [Fact]
    public void MessageQueue_HashCode_LegacySemantics()
    {
        var mq = new MessageQueue("A", "B", 0);
        Assert.Equal(93282, mq.HashCode());
    }

    [Fact]
    public void Bytes2String_UppercaseHex()
    {
        byte[] bs = { 0x00, 0x1F, 0xFF };
        Assert.Equal("001FFF", UtilAll.Bytes2String(bs));
        Assert.Equal(bs, UtilAll.String2Bytes("001FFF"));
        Assert.Empty(UtilAll.String2Bytes("ZZ"));
    }

    [Fact]
    public void StoredBodyCrc_Vector_For123456789_IsMasked()
    {
        // Stored BODYCRC = CRC-32/ISO-HDLC with the sign bit dropped.
        // "123456789": raw = 0xCBF43926 (high bit set) -> stored/masked = 0x4BF43926.
        byte[] data = Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xCBF43926u, RawCrc32(data));
        Assert.Equal(0x4BF43926u, UtilAll.Crc32(data));
    }

    [Fact]
    public void StoredBodyCrc_PinHighBitVector_SeparatesMaskedFromRaw()
    {
        // b"a": the raw CRC has the high bit set, so the masked stored value differs from the raw one.
        byte[] a = Encoding.ASCII.GetBytes("a");
        Assert.Equal(0xE8B7BE43u, RawCrc32(a));      // unmasked CRC-32/ISO-HDLC
        Assert.Equal(0x68B7BE43u, UtilAll.Crc32(a)); // stored BODYCRC (sign bit dropped)

        // b"abc": raw CRC already below 2^31, so masked == raw.
        byte[] abc = Encoding.ASCII.GetBytes("abc");
        Assert.Equal(0x352441C2u, RawCrc32(abc));
        Assert.Equal(0x352441C2u, UtilAll.Crc32(abc));
    }

    [Fact]
    public void StoredBodyCrc_DecodeFrameWithMaskedHighBitCrc_IsAccepted()
    {
        const string body = "crc-highbit-2";
        byte[] bodyBytes = Encoding.ASCII.GetBytes(body);

        // This body's raw CRC carries the high bit, so the stored field cannot equal the raw CRC.
        uint raw = RawCrc32(bodyBytes);
        Assert.True((raw & 0x8000_0000u) != 0, "raw CRC of the test body must have the high bit set");
        uint masked = raw & 0x7FFF_FFFFu;
        Assert.NotEqual(raw, masked);

        // Store frame whose BODYCRC field is the masked value the store actually writes.
        byte[] frame = HighBitBodyFrame(bodyBytes, masked);

        // Accepted: the check compares against the masked stored value. An unmasked CRC here would
        // disagree with the field and reject this valid message.
        Assert.True(MessageDecoder.DecodeMessage(frame, out MessageExt d,
            readBody: true, decompressBody: true, isClient: true, checkCrc: true));
        Assert.Equal(body, Encoding.ASCII.GetString(d.Body));
    }

    [Fact]
    public void StoredBodyCrc_DecodeFrameWithWrongBodyCrc_IsRejected()
    {
        const string body = "crc-highbit-2";
        byte[] bodyBytes = Encoding.ASCII.GetBytes(body);
        uint masked = RawCrc32(bodyBytes) & 0x7FFF_FFFFu;

        // Same frame, but the BODYCRC field deliberately off by one.
        byte[] frame = HighBitBodyFrame(bodyBytes, masked ^ 0x1u);

        Assert.False(MessageDecoder.DecodeMessage(frame, out _,
            readBody: true, decompressBody: true, isClient: true, checkCrc: true));

        // Without the CRC check the very same frame decodes fine: the rejection comes from the check.
        Assert.True(MessageDecoder.DecodeMessage(frame, out MessageExt d,
            readBody: true, decompressBody: true, isClient: true, checkCrc: false));
        Assert.Equal(body, Encoding.ASCII.GetString(d.Body));
    }

    private static byte[] HighBitBodyFrame(byte[] bodyBytes, uint storedBodyCrc)
    {
        var m = new MessageExt
        {
            Topic = "T_CRC",
            Body = bodyBytes,
            BodyCrc = storedBodyCrc,
            QueueId = 0,
            Flag = 0,
            QueueOffset = 0,
            CommitLogOffset = 0,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            ReconsumeTimes = 0,
        };
        return MessageDecoder.EncodeMessageExt(m, needCompress: false);
    }

    // Local unmasked CRC-32/ISO-HDLC, used only to show the masked stored value apart from the raw one.
    private static uint RawCrc32(byte[] data)
    {
        uint crc = 0xFFFF_FFFFu;
        foreach (byte b in data)
        {
            crc ^= b;
            for (int k = 0; k < 8; k++)
            {
                crc = (crc & 1) != 0 ? (0xEDB8_8320u ^ (crc >> 1)) : (crc >> 1);
            }
        }

        return crc ^ 0xFFFF_FFFFu;
    }

    [Fact]
    public void MsgId_RoundTrip()
    {
        byte[] addr = MessageDecoder.IpAndPortToBytes("127.0.0.1", 10911, false);
        Assert.Equal(8, addr.Length);
        string msgId = MessageDecoder.CreateMessageId(addr, 123456789L);
        Assert.True(MessageDecoder.DecodeMessageId(msgId, out string ip, out int port, out long off));
        Assert.Equal("127.0.0.1", ip);
        Assert.Equal(10911, port);
        Assert.Equal(123456789L, off);
    }
}
