// 消息二进制编解码（对应 org.apache.rocketmq.common.message.MessageDecoder）。
//
// 严格对齐 cpp/include/rocketmq/common/message_decoder.h 与 message_decoder.cpp
// （已对真实 5.5.1 集群验证），两条**互不可混用**的路径：
//   1) 17 段存储格式：EncodeMessageExt / DecodeMessage / DecodeMessages（broker 写入与 pull 返回）；
//   2) 6 段轻量格式：EncodeMessage / EncodeMessages / DecodeBatchMessage / DecodeBatchMessages（批量 body）。
//
// 压缩说明：与 Java/Python 对齐——消息体压缩用 **zlib 流格式**（RFC1950），
// 压缩类型取自 sysFlag 的 bit8~10；类型位为 0 的老版本消息按 ZLIB 解。
// 编解码统一走 CompressorFactory（见 Common/Compression.cs），
// 失败或不支持的类型会抛异常，而不是静默透传压缩字节。
//
// 17 段存储格式字段顺序与偏移（大端，单位字节）：
//   0  TOTALSIZE(4)
//   4  MAGICCODE(4, v1=-626843481 / v2=-626843477)
//   8  BODYCRC(4)
//   12 QUEUEID(4)
//   16 FLAG(4)
//   20 QUEUEOFFSET(8)
//   28 PHYSICALOFFSET(8)
//   36 SYSFLAG(4)
//   40 BORNTIMESTAMP(8)
//   48 BORNHOST(4|16B + port4)
//   56 STORETIMESTAMP(8)
//   64 STOREHOST(4|16B + port4)
//   72 RECONSUMETIMES(4)
//   76 PREPAREDTRANSACTIONOFFSET(8)
//   84 BODY(4 + len)
//      TOPIC(1B v1 / 2B v2 + bytes)
//      PROPERTIES(2 + len)
using System.Globalization;
using System.Text;

namespace RocketMQ.Common;

public static class MessageDecoder
{
    // 占位：C# 侧统一使用 UTF-8。
    private const int CharsetUtf8Separator = 0;
    private const byte NameValueSeparator = 1;
    private const byte PropertySeparator = 2;

    public const int MessageMagicCode = -626843481;
    public const int MessageMagicCodeV2 = -626843477;
    public const int BlankMagicCode = -875286124;

    // 字段固定偏移，与 Java MessageDecoder 常量一致。
    public const int MessageMagicCodePosition = 4;
    public const int MessageFlagPosition = 16;
    public const int MessagePhysicOffsetPosition = 28;
    public const int QueueOffsetPosition = 4 + 4 + 4 + 4 + 4;
    public const int PhyPosPosition = 4 + 4 + 4 + 4 + 4 + 8;
    public const int SysflagPosition = 4 + 4 + 4 + 4 + 4 + 8 + 8;
    public const int MessageStoreTimestampPosition = 56;

    // ---------------------------------------------------------------- 基础工具

    // IP + port -> 8B(v4) / 20B(v6)，与 broker 侧 InetSocketAddress 编码一致。
    public static byte[] IpAndPortToBytes(string ip, int port, bool v6 = false)
    {
        byte[] addr;
        if (!UtilAll.IpToBytes(ip, v6, out addr))
        {
            // 非法 IP 回退 127.0.0.1（保持长度与调用方期望一致）。
            UtilAll.IpToBytes("127.0.0.1", false, out addr);
            if (v6)
            {
                addr = new byte[16];
            }
        }

        var outBuf = new ByteWriter(addr.Length + 4);
        outBuf.WriteBytes(addr);
        outBuf.WriteInt32(port);
        return outBuf.ToArray();
    }

    // 8B / 20B -> (ip, port)。
    public static bool BytesToIpAndPort(byte[] raw, out string ip, out int port)
    {
        ip = string.Empty;
        port = 0;
        if (raw.Length == 8)
        {
            if (!UtilAll.BytesToIp(raw[0..4], out ip))
            {
                return false;
            }

            port = ByteReader.GetInt32At(raw, 4);
            return true;
        }

        if (raw.Length == 20)
        {
            if (!UtilAll.BytesToIp(raw[0..16], out ip))
            {
                return false;
            }

            port = ByteReader.GetInt32At(raw, 16);
            return true;
        }

        return false;
    }

    public static uint Crc32(byte[] data) => UtilAll.Crc32(data);

    public static string Bytes2String(byte[] bs) => UtilAll.Bytes2String(bs);

    // ------------------------------------------------------- 属性串 <-> Map

    // Java MessageDecoder.messageProperties2String：k\x01v\x02 逐项拼接。
    public static string MessagePropertiesToString(PropertyMap properties)
    {
        var outStr = new StringBuilder();
        foreach (var kv in properties)
        {
            outStr.Append(kv.Key);
            outStr.Append((char)NameValueSeparator);
            outStr.Append(kv.Value);
            outStr.Append((char)PropertySeparator);
        }

        return outStr.ToString();
    }

    // Java MessageDecoder.string2messageProperties。
    public static PropertyMap StringToMessageProperties(string propertiesStr)
    {
        var result = new PropertyMap();
        if (string.IsNullOrEmpty(propertiesStr))
        {
            return result;
        }

        int length = propertiesStr.Length;
        int index = 0;
        while (index < length)
        {
            int newIndex = propertiesStr.IndexOf((char)PropertySeparator, index);
            if (newIndex < 0)
            {
                newIndex = length;
            }

            if (newIndex - index >= 3)
            {
                int kvSep = propertiesStr.IndexOf((char)NameValueSeparator, index);
                if (kvSep >= 0 && kvSep > index && kvSep < newIndex - 1)
                {
                    result[propertiesStr.Substring(index, kvSep - index)] =
                        propertiesStr.Substring(kvSep + 1, newIndex - kvSep - 1);
                }
            }

            index = newIndex + 1;
        }

        return result;
    }

    // ---------------------------------------------------------------- msgId

    // ip+port(8 或 20B) + 8B commitLogOffset -> 大写十六进制 msgId。
    public static string CreateMessageId(byte[] addrBytes, long offset)
    {
        var raw = new ByteWriter(addrBytes.Length + 8);
        raw.WriteBytes(addrBytes);
        raw.WriteInt64(offset);
        return UtilAll.Bytes2String(raw.ToArray());
    }

    // Java MessageDecoder.decodeMessageId -> (ip, port, offset)。
    public static bool DecodeMessageId(string msgId, out string ip, out int port, out long offset)
    {
        ip = string.Empty;
        port = 0;
        offset = 0;
        byte[] raw = UtilAll.String2Bytes(msgId);
        if (raw.Length != 16 && raw.Length != 28)
        {
            return false;
        }

        int ipLen = raw.Length == 16 ? 4 : 16;
        if (!UtilAll.BytesToIp(raw[0..ipLen], out ip))
        {
            return false;
        }

        port = ByteReader.GetInt32At(raw, ipLen);
        offset = ByteReader.GetInt64At(raw, ipLen + 4);
        return true;
    }

    // ------------------------------------------- 1) 17 段存储格式：MessageExt

    public static byte[] EncodeMessageExt(MessageExt messageExt, bool needCompress = false)
    {
        // 压缩：仅在 needCompress 且 sysFlag 已置 COMPRESSED_FLAG 时执行。
        // 与 Java MessageDecoder.encode 一致，压缩类型取自 sysFlag 的 bit8~10。
        byte[] body = MaybeCompress(messageExt.Body, needCompress, messageExt.SysFlag);
        int bodyLength = body.Length;

        byte[] topicBytes = Encoding.UTF8.GetBytes(messageExt.Topic);
        int topicLen = topicBytes.Length;
        byte[] propBytes = Encoding.UTF8.GetBytes(MessagePropertiesToString(messageExt.Properties));
        int propertiesLength = propBytes.Length;

        int sysFlag = messageExt.SysFlag;
        int bornhostLength = (sysFlag & MessageSysFlag.BornhostV6Flag) != 0 ? 20 : 8;
        int storehostLength = (sysFlag & MessageSysFlag.StorehostaddressV6Flag) != 0 ? 20 : 8;

        int computedSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8
                        + bornhostLength + storehostLength + 4 + 8
                        + 4 + bodyLength
                        + 1 + topicLen
                        + 2 + propertiesLength;
        int storeSize = messageExt.StoreSize > 0 ? messageExt.StoreSize : computedSize;
        if (storeSize < computedSize)
        {
            storeSize = computedSize;
        }

        string bornHost = string.IsNullOrEmpty(messageExt.BornHost) ? "127.0.0.1" : messageExt.BornHost;
        int bornPort = messageExt.BornHostPort;
        string storeHost = string.IsNullOrEmpty(messageExt.StoreHost) ? "127.0.0.1" : messageExt.StoreHost;
        int storePort = messageExt.StoreHostPort;

        var buf = new ByteWriter();
        buf.WriteInt32(storeSize);                                              // 1 TOTALSIZE
        buf.WriteInt32(MessageMagicCode);                                       // 2 MAGICCODE
        buf.WriteUInt32(messageExt.BodyCrc);                                    // 3 BODYCRC
        buf.WriteInt32(messageExt.QueueId);                                     // 4 QUEUEID
        buf.WriteInt32(messageExt.Flag);                                       // 5 FLAG
        buf.WriteInt64(messageExt.QueueOffset);                                 // 6 QUEUEOFFSET
        buf.WriteInt64(messageExt.CommitLogOffset);                             // 7 PHYSICALOFFSET
        buf.WriteInt32(sysFlag);                                                // 8 SYSFLAG
        buf.WriteInt64(messageExt.BornTimestamp);                               // 9 BORNTIMESTAMP
        buf.WriteBytes(IpAndPortToBytes(bornHost, bornPort,
            (sysFlag & MessageSysFlag.BornhostV6Flag) != 0));                  // 10 BORNHOST
        buf.WriteInt64(messageExt.StoreTimestamp);                              // 11 STORETIMESTAMP
        buf.WriteBytes(IpAndPortToBytes(storeHost, storePort,
            (sysFlag & MessageSysFlag.StorehostaddressV6Flag) != 0));          // 12 STOREHOST
        buf.WriteInt32(messageExt.ReconsumeTimes);                              // 13 RECONSUMETIMES
        buf.WriteInt64(messageExt.PreparedTransactionOffset);                   // 14
        buf.WriteInt32(bodyLength);                                            // 15 BODY
        buf.WriteBytes(body);
        buf.WriteInt8((sbyte)(topicLen & 0xFF));                                // 16 TOPIC
        buf.WriteBytes(topicBytes);
        buf.WriteUInt16((ushort)(propertiesLength & 0xFFFF));                   // 17 PROPERTIES
        buf.WriteBytes(propBytes);
        return buf.ToArray();
    }

    // 解码失败返回 false（对应 Java decode 返回 null）。
    public static bool DecodeMessage(byte[] raw, out MessageExt outMsg, bool readBody = true,
        bool decompressBody = true, bool isClient = true, bool checkCrc = false)
    {
        outMsg = new MessageExt();
        try
        {
            var r = new ByteReader(raw);
            int storeSize = r.ReadInt32();
            int magicCode = r.ReadInt32();
            if (magicCode != MessageMagicCode && magicCode != MessageMagicCodeV2)
            {
                return false; // 未知魔数 -> 解码失败（对应 Java 返回 null）
            }

            bool useV2 = magicCode == MessageMagicCodeV2;

            uint bodyCrc = r.ReadUnsignedInt32();
            int queueId = r.ReadInt32();
            int flag = r.ReadInt32();
            long queueOffset = r.ReadInt64();
            long physicOffset = r.ReadInt64();
            int sysFlag = r.ReadInt32();
            long bornTimestamp = r.ReadInt64();

            int bornhostLen = (sysFlag & MessageSysFlag.BornhostV6Flag) != 0 ? 20 : 8;
            string bornHost = string.Empty;
            int bornPort = 0;
            if (!BytesToIpAndPort(r.ReadBytes(bornhostLen), out bornHost, out bornPort))
            {
                return false;
            }

            long storeTimestamp = r.ReadInt64();
            int storehostLen = (sysFlag & MessageSysFlag.StorehostaddressV6Flag) != 0 ? 20 : 8;
            string storeHost = string.Empty;
            int storePort = 0;
            if (!BytesToIpAndPort(r.ReadBytes(storehostLen), out storeHost, out storePort))
            {
                return false;
            }

            int reconsumeTimes = r.ReadInt32();
            long preparedTransactionOffset = r.ReadInt64();

            outMsg.StoreSize = storeSize;
            outMsg.BodyCrc = bodyCrc;
            outMsg.QueueId = queueId;
            outMsg.Flag = flag;
            outMsg.QueueOffset = queueOffset;
            outMsg.CommitLogOffset = physicOffset;
            outMsg.SysFlag = sysFlag;
            outMsg.BornTimestamp = bornTimestamp;
            outMsg.BornHost = bornHost;
            outMsg.BornHostPort = bornPort;
            outMsg.StoreTimestamp = storeTimestamp;
            outMsg.StoreHost = storeHost;
            outMsg.StoreHostPort = storePort;
            outMsg.ReconsumeTimes = reconsumeTimes;
            outMsg.PreparedTransactionOffset = preparedTransactionOffset;

            // 15 BODY
            int bodyLen = r.ReadInt32();
            if (bodyLen > 0)
            {
                if (readBody)
                {
                    byte[] body = r.ReadBytes(bodyLen);
                    if (checkCrc && Crc32(body) != bodyCrc)
                    {
                        return false;
                    }

                    // 解压：与 Java MessageDecoder.decode 一致，压缩类型取自 sysFlag 的 bit8~10。
                    // 类型位为 0 的老版本压缩消息由 CompressionType.findByValue 映射到 ZLIB。
                    // **失败或不支持的类型会抛异常**，绝不返回压缩字节——否则上层会把压缩流当正文，
                    // 属静默数据损坏（Java 同样是抛 IOException / RuntimeException）。
                    body = MaybeDecompress(body, decompressBody, sysFlag);
                    outMsg.Body = body;
                    outMsg.HasBody = true;
                    // 对齐 Java：解压成功后清掉 COMPRESSED_FLAG（保留 bit8~10 的类型位）。
                    // 条件与 Java `if (deCompressBody && isCompressed(sysFlag))` 完全相同。
                    if (decompressBody && MessageSysFlag.IsCompressed(sysFlag))
                    {
                        outMsg.SysFlag = MessageSysFlag.ClearCompressedFlag(sysFlag);
                    }
                }
                else
                {
                    r.Skip(bodyLen);
                    outMsg.Body = Array.Empty<byte>();
                    outMsg.HasBody = false;
                }
            }
            else
            {
                outMsg.Body = Array.Empty<byte>();
                outMsg.HasBody = false;
            }

            // 16 TOPIC
            int topicLen = useV2 ? r.ReadInt16() : r.ReadUnsignedInt8();
            outMsg.Topic = Encoding.UTF8.GetString(r.ReadBytes(topicLen));

            // 17 PROPERTIES
            int propertiesLength = r.ReadInt16();
            if (propertiesLength > 0)
            {
                outMsg.Properties = StringToMessageProperties(
                    Encoding.UTF8.GetString(r.ReadBytes(propertiesLength)));
            }
            else
            {
                outMsg.Properties = new PropertyMap();
            }

            // msgId = storeHost(ip+port) + commitLogOffset
            byte[] storeAddrRaw = IpAndPortToBytes(storeHost, storePort, storehostLen == 20);
            outMsg.MsgId = CreateMessageId(storeAddrRaw, physicOffset);
            if (isClient)
            {
                outMsg.OffsetMsgId = outMsg.MsgId;
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    // 消息流 -> MessageExt 列表（对应 MessageDecoder.decodes，用于 pull 结果）。
    public static List<MessageExt> DecodeMessages(byte[] raw, bool readBody = true)
    {
        var result = new List<MessageExt>();
        int pos = 0;
        int total = raw.Length;
        while (pos < total)
        {
            if (total - pos < 4)
            {
                break;
            }

            int storeSize = ByteReader.GetInt32At(raw, pos);
            if (storeSize <= 0 || storeSize > total - pos)
            {
                break;
            }

            if (!DecodeMessage(raw[pos..(pos + storeSize)], out MessageExt msg, readBody))
            {
                break;
            }

            result.Add(msg);
            pos += storeSize;
        }

        return result;
    }

    // --------------------------------------- 2) 6 段轻量格式：批量消息 body

    // Java MessageDecoder.encodeMessage(Message)。
    public static byte[] EncodeMessage(Message message)
    {
        byte[] body = message.Body;
        byte[] propBytes = Encoding.UTF8.GetBytes(MessagePropertiesToString(message.Properties));
        int propertiesLength = propBytes.Length;
        // TOTALSIZE(4) + MAGICCODE(4) + BODYCRC(4) + FLAG(4) + BODYLEN(4) + body + PROPLEN(2) + props
        int storeSize = 4 + 4 + 4 + 4 + 4 + body.Length + 2 + propertiesLength;

        var buf = new ByteWriter();
        buf.WriteInt32(storeSize);                                     // 1 TOTALSIZE
        buf.WriteInt32(0);                                             // 2 MAGICCODE（批量场景固定 0）
        buf.WriteInt32(0);                                             // 3 BODYCRC
        buf.WriteInt32(message.Flag);                                  // 4 FLAG
        buf.WriteInt32(body.Length);                                   // 5 BODY
        buf.WriteBytes(body);
        buf.WriteUInt16((ushort)(propertiesLength & 0xFFFF));          // 6 PROPERTIES
        buf.WriteBytes(propBytes);
        return buf.ToArray();
    }

    // Java MessageDecoder.encodeMessages(List<Message>)。
    public static byte[] EncodeMessages(List<Message> messages)
    {
        var outBuf = new ByteWriter();
        foreach (var m in messages)
        {
            outBuf.WriteBytes(EncodeMessage(m));
        }

        return outBuf.ToArray();
    }

    // Java MessageDecoder.decodeMessage(ByteBuffer)：单条批量单元 -> Message。
    public static bool DecodeBatchMessage(byte[] raw, out Message outMsg)
    {
        outMsg = new Message();
        try
        {
            var r = new ByteReader(raw);
            r.Skip(4); // TOTALSIZE
            r.Skip(4); // MAGICCODE
            r.Skip(4); // BODYCRC
            int flag = r.ReadInt32();
            int bodyLen = r.ReadInt32();
            byte[] body = r.ReadBytes(bodyLen);
            int propertiesLen = r.ReadInt16();
            PropertyMap properties = new PropertyMap();
            if (propertiesLen > 0)
            {
                properties = StringToMessageProperties(
                    Encoding.UTF8.GetString(r.ReadBytes(propertiesLen)));
            }

            outMsg.Flag = flag;
            outMsg.Body = body;
            outMsg.HasBody = true;
            outMsg.Properties = properties;
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    // Java MessageDecoder.decodeMessages(ByteBuffer)：批量 body -> Message 列表。
    public static List<Message> DecodeBatchMessages(byte[] raw)
    {
        var result = new List<Message>();
        int pos = 0;
        int total = raw.Length;
        while (pos < total)
        {
            if (total - pos < 4)
            {
                break;
            }

            int storeSize = ByteReader.GetInt32At(raw, pos);
            if (storeSize <= 0 || storeSize > total - pos)
            {
                break;
            }

            if (!DecodeBatchMessage(raw[pos..(pos + storeSize)], out Message msg))
            {
                break;
            }

            result.Add(msg);
            pos += storeSize;
        }

        return result;
    }

    // Java MessageDecoder.countInnerMsgNum。
    public static int CountInnerMsgNum(byte[] raw)
    {
        int count = 0;
        int pos = 0;
        int total = raw.Length;
        while (pos < total)
        {
            count += 1;
            if (total - pos < 4)
            {
                break;
            }

            int size = ByteReader.GetInt32At(raw, pos);
            if (size <= 0 || size > total - pos)
            {
                break;
            }

            pos += size;
        }

        return count;
    }

    // ---------------------------------------------------------------- 内部辅助

    // 压缩：仅在 needCompress 且 sysFlag 已置 COMPRESSED_FLAG 时执行。
    private static byte[] MaybeCompress(byte[] body, bool needCompress, int sysFlag)
    {
        if (!needCompress)
        {
            return body;
        }

        if (!MessageSysFlag.IsCompressed(sysFlag))
        {
            return body;
        }

        int type = MessageSysFlag.GetCompressionType(sysFlag);
        return CompressorFactory.Compress(body, type, GetCompressLevel());
    }

    // 解压：与 Java MessageDecoder.decode 一致，压缩类型取自 sysFlag 的 bit8~10。
    // 类型位为 0 的老版本压缩消息由 CompressionType.findByValue 映射到 ZLIB。
    // **失败或不支持的类型会抛异常**，绝不返回压缩字节——否则上层会把压缩流当正文，
    // 属静默数据损坏（Java 同样是抛 IOException / RuntimeException）。
    private static byte[] MaybeDecompress(byte[] body, bool decompressBody, int sysFlag)
    {
        if (!decompressBody)
        {
            return body;
        }

        if (!MessageSysFlag.IsCompressed(sysFlag))
        {
            return body;
        }

        int type = MessageSysFlag.GetCompressionType(sysFlag);
        return CompressorFactory.Decompress(body, type);
    }

    // 压缩级别从环境变量 rocketmq.message.compressLevel 取（默认 5，与 Java 一致）。
    private static int GetCompressLevel()
    {
        string? v = Environment.GetEnvironmentVariable(MixAll.MessageCompressLevel);
        if (v != null && int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out int lvl))
        {
            if (lvl >= 0 && lvl <= 9)
            {
                return lvl;
            }
        }

        return 5;
    }
}
