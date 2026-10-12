// 句柄不是客户端自己造的：发送带 TIMER_DELIVER_MS / TIMER_DELAY_MS / TIMER_DELAY_SEC
// 的定时消息时，broker 在 SendMessageProcessor#attachRecallHandle 里把这个句柄挂到
// SEND 响应头的 recallHandle 字段上返回，客户端只负责原样带回去调用 RecallMessage。
// 普通消息的响应里根本没有这个字段。
using System.Text;

namespace RocketMQ.Client;

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

    public const string InvalidHandle = "recall handle is invalid";

    private static readonly UTF8Encoding StrictUtf8 = new(false);

    public static string BuildHandle(string topic, string brokerName, string timestampStr,
                                     string messageId)
    {
        string raw = Version1 + Separator + topic + Separator + brokerName + Separator
                     + timestampStr + Separator + messageId;
        return Convert.ToBase64String(StrictUtf8.GetBytes(raw))
            .Replace('+', '-')
            .Replace('/', '_');
    }

    public static HandleV1 DecodeHandle(string? handle)
    {
        if (string.IsNullOrEmpty(handle))
        {
            throw new MQClientException(InvalidHandle);
        }
        byte[] raw;
        try
        {
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
