using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <b>Why intercept at the client</b>: when a topic/group name is illegal the broker also rejects it, but only after the request
/// actually goes out and you get <c>TOPIC_NOT_EXIST</c>/ILLEGAL_TOPIC, and <c>TOPIC_NOT_EXIST</c> is in the
/// <b>retriable code</b> set for send retries — so every doomed message burns through the retry count and timeout budget once,
/// and the final error is still the same cause. Local validation makes such input fail on the first line of <c>Send()</c>.
public static class Validators
{
    public const int CharacterMaxLength = 255;

    private static readonly string FileSeparator = System.IO.Path.DirectorySeparatorChar.ToString();

    private static string Inv(int v) => v.ToString(CultureInfo.InvariantCulture);

    /// <summary>Corresponds to <c>Validators.checkGroup</c>.</summary>
    public static void CheckGroup(string? group)
    {
        if (UtilAll.IsBlank(group))
        {
            throw new MQClientException("the specified group is blank");
        }

        if (group!.Length > TopicValidator.GroupMaxLength)
        {
            throw new MQClientException("the specified group[" + group + "] is longer than group max length: "
                                        + Inv(TopicValidator.GroupMaxLength) + ".");
        }

        if (TopicValidator.IsTopicOrGroupIllegal(group))
        {
            throw new MQClientException("the specified group[" + group
                                        + "] contains illegal characters, allowing only "
                                        + TopicValidator.ValidCharPattern);
        }
    }

    /// <summary>Corresponds to <c>Validators.checkTopic</c>.</summary>
    public static void CheckTopic(string? topic)
    {
        if (UtilAll.IsBlank(topic))
        {
            throw new MQClientException("The specified topic is blank");
        }

        if (topic!.Length > TopicValidator.TopicMaxLength)
        {
            throw new MQClientException("The specified topic is longer than topic max length "
                                        + Inv(TopicValidator.TopicMaxLength) + ".");
        }

        if (TopicValidator.IsTopicOrGroupIllegal(topic))
        {
            throw new MQClientException("The specified topic[" + topic
                                        + "] contains illegal characters, allowing only "
                                        + TopicValidator.ValidCharPattern);
        }
    }

    /// <summary>Corresponds to <c>Validators.isSystemTopic</c>: throws directly when a system topic is hit.</summary>
    public static void IsSystemTopic(string topic)
    {
        if (TopicValidator.IsSystemTopic(topic))
        {
            throw new MQClientException("The topic[" + topic + "] is conflict with system topic.");
        }
    }

    /// <summary>Corresponds to <c>Validators.isNotAllowedSendTopic</c>.</summary>
    public static void IsNotAllowedSendTopic(string topic)
    {
        if (TopicValidator.IsNotAllowedSendTopic(topic))
        {
            throw new MQClientException("Sending message to topic[" + topic + "] is forbidden.");
        }
    }

    public static void CheckMessage(Message? msg, int maxMessageSize)
    {
        if (msg is null)
        {
            throw new MQClientException("the message is null", ResponseCode.MessageIllegal);
        }

        CheckTopic(msg.Topic);
        IsNotAllowedSendTopic(msg.Topic);

        if (msg.Body is null)
        {
            throw new MQClientException("the message body is null", ResponseCode.MessageIllegal);
        }

        if (msg.Body.Length == 0)
        {
            throw new MQClientException("the message body length is zero", ResponseCode.MessageIllegal);
        }

        if (msg.Body.Length > maxMessageSize)
        {
            throw new MQClientException(
                "the message body size over max value, MAX: " + Inv(maxMessageSize),
                ResponseCode.MessageIllegal);
        }

        // In multi-queue dispatch (LMQ) paths, a filesystem separator would make the broker build an out-of-bounds path when creating queues
        string lmqPath = msg.GetProperty(MessageConst.PropertyInnerMultiDispatch);
        if (lmqPath.Length > 0 && lmqPath.Contains(FileSeparator, StringComparison.Ordinal))
        {
            throw new MQClientException(
                "INNER_MULTI_DISPATCH " + lmqPath + " can not contains " + FileSeparator + " character",
                ResponseCode.MessageIllegal);
        }
    }
}
