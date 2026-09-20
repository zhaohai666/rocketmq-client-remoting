// org.apache.rocketmq.client.Validators 的 C# 对应：发送/订阅入口上的名字校验。
using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>
/// 发送/订阅入口上的名字校验（对应 <c>org.apache.rocketmq.client.Validators</c>）。
///
/// <b>为什么要在客户端就拦下来</b>：topic/group 名字非法时 broker 也会拒，但要等到请求
/// 真的打出去才拿到 <c>TOPIC_NOT_EXIST</c>/ILLEGAL_TOPIC，而 <c>TOPIC_NOT_EXIST</c> 在
/// 发送重试的<b>可重试码</b>集合里 —— 于是每条必然失败的消息都会把重试次数和超时预算空转
/// 一遍，最后报的还是同一个原因。本地校验让这类输入在 <c>Send()</c> 的第一行就失败。
///
/// 逐条对齐 Java：
/// <list>
///   <item><c>CheckTopic</c> / <c>CheckGroup</c> / 禁发 topic / 系统 topic：这几步在 Java 走的是
///     <c>MQClientException(String, Throwable)</c> 构造器，responseCode 为 -1（纯客户端错误）；
///     这里沿用本工程既有的客户端异常默认码 1，只有 <c>CheckMessage</c> 的 body 四条需要精确码。</item>
///   <item><c>CheckMessage</c>：只有它带 <c>ResponseCode.MESSAGE_ILLEGAL</c>(13)，且<b>顺序</b>要对——
///     Java 先查 topic、再查禁发 topic、最后才查 body。</item>
///   <item><c>IsNotAllowedSendTopic</c>：只禁 broker 内部流水那 8 个。<b><c>%RETRY%</c> 不在名单里</b>：
///     <c>SendMessageBack</c> 就是往 <c>%RETRY%group</c> 写的，禁掉会打断重投链路。</item>
/// </list>
/// </summary>
public static class Validators
{
    /// <summary>对应 Java <c>Validators.CHARACTER_MAX_LENGTH</c>，本工程未用到但保留常量口径。</summary>
    public const int CharacterMaxLength = 255;

    // 对应 Java 的 File.separator：批量/LMQ 路径校验按当前平台分隔符判定。
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

    /// <summary>
    /// 对应 <c>Validators.checkMessage(msg, producer)</c>（顺序与文案逐条照抄）。
    /// Java 从 producer 上取 maxMessageSize，这里直接收数值，避免 Client 层循环依赖具体生产者类型。
    /// </summary>
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
