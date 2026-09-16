// 客户端层异常（对应 org.apache.rocketmq.client.exception.* 与 Python client/exception.py）。
namespace RocketMQ.Client;

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
