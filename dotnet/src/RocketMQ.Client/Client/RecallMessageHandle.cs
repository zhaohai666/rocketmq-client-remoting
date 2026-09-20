// 定时消息的撤回句柄（对应 org.apache.rocketmq.common.producer.RecallMessageHandle）。
//
// 句柄不是客户端自己造的：发送带 TIMER_DELIVER_MS / TIMER_DELAY_MS / TIMER_DELAY_SEC
// 的定时消息时，broker 在 SendMessageProcessor#attachRecallHandle 里把这个句柄挂到
// SEND 响应头的 recallHandle 字段上返回，客户端只负责原样带回去调用 RecallMessage。
// 普通消息的响应里根本没有这个字段。
//
// 编码格式与 Java 完全一致：base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")，
// 5 段、空格分隔。
//
// 为什么放在 Client 而不是 Common：Java 把它放在 common/producer，但本端口约定
// Common 不反向依赖 Client 的 MQClientException（见 ConsistentHash.cs 的同一说明）；
// 解码失败要抛的就是 MQClientException，所以落在 Client。
//
// 与 Java 的两处显式差异：
//   * Java buildHandle 用 Base64.getUrlEncoder()（**带** '=' 填充），decodeHandle 用
//     getUrlDecoder()（严格，无填充串会抛错）。这里编码同样带填充，解码两种都吃：
//     另外三个客户端移植版用无填充解码器，只发无填充句柄的客户端写下的消息也要能撤回。
//   * Java 的 new String(bytes, UTF_8) 对非法字节序列是替换而不是报错，这里与 Rust/C++
//     一致：严格校验 utf-8，非法直接判「句柄非法」。差别只在畸形句柄上。
using System.Text;

namespace RocketMQ.Client;

/// <summary>对应 Java RecallMessageHandle.HandleV1。</summary>
/// <remarks>
/// TimestampStr 保留字符串而不是转成 long：Java 就存 String，撤回时要原样回填，
/// 非法时间戳由 broker 判 ILLEGAL_OPERATION。
/// </remarks>
public sealed class HandleV1
{
    public string Topic { get; set; } = string.Empty;
    public string BrokerName { get; set; } = string.Empty;
    public string TimestampStr { get; set; } = string.Empty;
    public string MessageId { get; set; } = string.Empty;
}

public static class RecallMessageHandle
{
    public const string Separator = " ";
    public const string Version1 = "v1";

    /// <summary>Java 在所有解码失败分支上给的就是这一句。</summary>
    public const string InvalidHandle = "recall handle is invalid";

    /// <summary>UTF8Encoding 的 decoder 默认就是 ExceptionFallback，非法字节会抛
    /// DecoderFallbackException；Java 的 new String(bytes, UTF_8) 则是替换字符。</summary>
    private static readonly UTF8Encoding StrictUtf8 = new(false);

    /// <summary>对应 RecallMessageHandle.buildHandle（输出带 '=' 填充，与 Java 一致）。</summary>
    public static string BuildHandle(string topic, string brokerName, string timestampStr,
                                     string messageId)
    {
        string raw = Version1 + Separator + topic + Separator + brokerName + Separator
                     + timestampStr + Separator + messageId;
        return Convert.ToBase64String(StrictUtf8.GetBytes(raw))
            .Replace('+', '-')
            .Replace('/', '_');
    }

    /// <summary>
    /// 对应 RecallMessageHandle.decodeHandle：空串 / 非法 base64 / 非 utf-8 /
    /// 首段不是 "v1" / 段数 &lt; 5 都抛 MQClientException("recall handle is invalid")。
    /// 超过 5 段时忽略尾段（Java 的 split 取 items[1..4] 同样忽略）。
    /// </summary>
    public static HandleV1 DecodeHandle(string? handle)
    {
        if (string.IsNullOrEmpty(handle))
        {
            throw new MQClientException(InvalidHandle);
        }
        byte[] raw;
        try
        {
            // base64url → 标准 base64，并补齐 '='（Java 的 getUrlDecoder 不接受无填充串，
            // 本端口两种都吃，见文件头差异说明）。
            string standard = handle!.Replace('-', '+').Replace('_', '/');
            int pad = standard.Length % 4;
            if (pad == 2) standard += "==";
            else if (pad == 3) standard += "=";
            else if (pad != 0) throw new FormatException();
            raw = Convert.FromBase64String(standard);
        }
        catch (FormatException)
        {
            throw new MQClientException(InvalidHandle);
        }

        string text;
        try
        {
            text = StrictUtf8.GetString(raw);
        }
        catch (DecoderFallbackException)
        {
            throw new MQClientException(InvalidHandle);
        }

        // Java: split(" ") 后 items[0] 必须是 v1 且长度 >= 5，取 items[1..4]。
        string[] items = text.Split(Separator.ToCharArray());
        if (items.Length < 5 || items[0] != Version1)
        {
            throw new MQClientException(InvalidHandle);
        }
        return new HandleV1
        {
            Topic = items[1],
            BrokerName = items[2],
            TimestampStr = items[3],
            MessageId = items[4],
        };
    }
}
