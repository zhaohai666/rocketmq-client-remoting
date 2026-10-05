// 客户端层异常（对应 org.apache.rocketmq.client.exception.* 与 Python client/exception.py）。
namespace RocketMQ.Client;

/// <summary>
/// 对应 org.apache.rocketmq.client.common.ClientErrorCode（Python 同名类）。
///
/// 七个常量与 Java 一一对应。sendDefaultImpl 重试耗尽后用它给最终的 MQClientException
/// 定性：连不上 broker→10001，等响应超时→10002，客户端自身问题→10003，
/// 地址服务器没给地址→10004，路由查不到→10005。
/// 另两个不在重试定性里，各有各的抛出点：
/// <list type="bullet">
/// <item>10006 —— request-reply 等到点没等到应答（Java DefaultMQProducerImpl#waitResponse）。</item>
/// <item>10007 —— 由请求消息造应答消息失败（Java MessageUtil#createReplyMessage）。</item>
/// </list>
/// </summary>
public static class ClientErrorCode
{
    public const int ConnectBrokerException = 10001;
    public const int AccessBrokerTimeout = 10002;
    public const int BrokerNotExistException = 10003;
    public const int NoNameServerException = 10004;
    public const int NotFoundTopicException = 10005;
    public const int RequestTimeoutException = 10006;
    public const int CreateReplyMessageException = 10007;
}

/// <summary>
/// 对应 Python MQClientException(message, responseCode)；默认 1（UNKNOWN）。
/// 管理端靠它把 broker 响应码透传出来——例如 resetOffsetNew 需要区分
/// CONSUMER_NOT_ONLINE 才能退化到 resetOffsetByTimestampOld。
/// </summary>
public class MQClientException : Exception
{
    public int ResponseCode { get; }

    public MQClientException(string msg, int code = 1) : base(msg) => ResponseCode = code;

    public MQClientException(string msg, Exception? inner, int code = 1) : base(msg, inner) =>
        ResponseCode = code;
}

/// <summary>对应 Java MQBrokerException：带 broker 返回的 responseCode。</summary>
public class MQBrokerException : Exception
{
    // 对应 Java ResponseCode.SYSTEM_ERROR / Python 的 UNKNOWN
    public const int Unknown = 1;

    public int ResponseCode { get; }

    public string ResponseMessage { get; }

    public MQBrokerException(int code, string msg)
        : base("CODE: " + code.ToString(System.Globalization.CultureInfo.InvariantCulture) + " DESC: " + msg)
    {
        ResponseCode = code;
        ResponseMessage = msg;
    }
}

/// <summary>对应 Java MQClientException 的 "no route info" 语义化子类（此处仅作语义标记）。</summary>
public class MQClientNoRouteException : MQClientException
{
    public MQClientNoRouteException(string topic) : base("No route info of this topic: " + topic)
    {
    }
}
