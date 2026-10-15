namespace RocketMQ.Remoting;

/// <summary>Basic remoting exception.</summary>
public class RemotingException : Exception
{
    public RemotingException(string message) : base(message)
    {
    }
}

public class RemotingConnectException : RemotingException
{
    public RemotingConnectException(string message) : base(message)
    {
    }
}

public class RemotingSendRequestException : RemotingException
{
    public RemotingSendRequestException(string message) : base(message)
    {
    }
}

/// <summary>
/// "Expected" timeouts such as long-polling timeouts. Upper layers treat it as benign DEBUG —
/// the broker clamps the issued suspend down to its own brokerSuspendMaxTimeMillis, so periodic
/// timeouts on idle queues are normal behavior.
/// </summary>
public class RemotingTimeoutException : RemotingException
{
    public RemotingTimeoutException(string message) : base(message)
    {
    }
}

/// <summary>Frame/header decode failure.</summary>
public class RemotingCommandException : RemotingException
{
    public RemotingCommandException(string message) : base(message)
    {
    }
}

public class RemotingTooMuchRequestException : RemotingException
{
    public RemotingTooMuchRequestException(string message) : base(message)
    {
    }
}
