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
/// <summary>
/// MQClientException(message, responseCode) semantics; defaults to 1 (UNKNOWN).
/// The admin side relies on it to pass broker response codes through — e.g.
/// resetOffsetNew needs to distinguish CONSUMER_NOT_ONLINE before it can fall
/// back to resetOffsetByTimestampOld.
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
