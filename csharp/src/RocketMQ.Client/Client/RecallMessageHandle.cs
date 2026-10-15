// The handle is not created by the client itself: when sending a scheduled message carrying TIMER_DELIVER_MS / TIMER_DELAY_MS / TIMER_DELAY_SEC,
// the broker attaches this handle in SendMessageProcessor#attachRecallHandle and returns it on the
// recallHandle field of the SEND response header; the client only has to carry it back verbatim when calling RecallMessage.
// Regular messages' responses do not contain this field at all.
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
