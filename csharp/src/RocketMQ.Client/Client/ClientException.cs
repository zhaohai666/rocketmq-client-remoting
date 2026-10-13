namespace RocketMQ.Client;

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
/// MQClientException(message, responseCode) 语义；默认 1（UNKNOWN）。
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

public class MQBrokerException : Exception
{
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

public class MQClientNoRouteException : MQClientException
{
    public MQClientNoRouteException(string topic) : base("No route info of this topic: " + topic)
    {
    }
}
