// 异常类型（对应 org.apache.rocketmq.remoting.exception.*）。
namespace RocketMQ.Remoting;

/// <summary>基础远程调用异常。</summary>
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
/// 长轮询超时等「预期内」超时。上层把它当良性 DEBUG 处理——
/// broker 会把下发的 suspend 钳制到自身 brokerSuspendMaxTimeMillis，空闲队列周期性超时是正常行为。
/// </summary>
public class RemotingTimeoutException : RemotingException
{
    public RemotingTimeoutException(string message) : base(message)
    {
    }
}

/// <summary>帧/头部解码失败。</summary>
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
