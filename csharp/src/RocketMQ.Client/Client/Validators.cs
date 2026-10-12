using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <b>为什么要在客户端就拦下来</b>：topic/group 名字非法时 broker 也会拒，但要等到请求
/// 真的打出去才拿到 <c>TOPIC_NOT_EXIST</c>/ILLEGAL_TOPIC，而 <c>TOPIC_NOT_EXIST</c> 在
/// 发送重试的<b>可重试码</b>集合里 —— 于是每条必然失败的消息都会把重试次数和超时预算空转
/// 一遍，最后报的还是同一个原因。本地校验让这类输入在 <c>Send()</c> 的第一行就失败。
public static class Validators
{
    public const int CharacterMaxLength = 255;

    private static readonly string FileSeparator = System.IO.Path.DirectorySeparatorChar.ToString();

    private static string Inv(int v) => v.ToString(CultureInfo.InvariantCulture);

    /// <summary>对应 <c>Validators.checkGroup</c>。</summary>
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

    /// <summary>对应 <c>Validators.checkTopic</c>。</summary>
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

    /// <summary>对应 <c>Validators.isSystemTopic</c>：命中系统 topic 直接抛。</summary>
    public static void IsSystemTopic(string topic)
    {
        if (TopicValidator.IsSystemTopic(topic))
        {
            throw new MQClientException("The topic[" + topic + "] is conflict with system topic.");
        }
    }

    /// <summary>对应 <c>Validators.isNotAllowedSendTopic</c>。</summary>
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

        // 多队列分发（LMQ）的路径里带文件系统分隔符会让 broker 侧建队列时拼出越界路径
        string lmqPath = msg.GetProperty(MessageConst.PropertyInnerMultiDispatch);
        if (lmqPath.Length > 0 && lmqPath.Contains(FileSeparator, StringComparison.Ordinal))
        {
            throw new MQClientException(
                "INNER_MULTI_DISPATCH " + lmqPath + " can not contains " + FileSeparator + " character",
                ResponseCode.MessageIllegal);
        }
    }
}
