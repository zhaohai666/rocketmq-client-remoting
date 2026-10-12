// 两个关键点（错了真机就不通）：
// 1. 应答消息必须带 MSG_TYPE == "reply"，发送时据此把请求码从 SEND_MESSAGE_V2(310)
//    换成 SEND_REPLY_MESSAGE_V2(325)；broker 的 ReplyMessageProcessor 只在 324/325 注册。
// 2. REPLY_TO_CLIENT 是请求方的 clientId，broker 靠它在 producerManager 里反查 channel
//    才能把应答推回——所以请求方必须先发过心跳（已注册为 producer）。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Client;

public class RequestTimeoutException : MQClientException
{
    public RequestTimeoutException(string msg)
        : base(msg, ClientErrorCode.RequestTimeoutException)
    {
    }
}

public abstract class RequestCallback
{
    public abstract void OnSuccess(Message? responseMessage);

    public abstract void OnException(Exception? e);
}

public sealed class RequestResponseFuture
{
    public string CorrelationId { get; }

    public int TimeoutMillis { get; }

    public RequestCallback? RequestCallback { get; }

    public long BeginTimestamp { get; }

    public Message? ResponseMsg { get; private set; }

    /// <summary>发送是否成功（失败路径置 false，用于区分超时 vs 发送失败）。</summary>
    public bool SendRequestOk { get; set; } = true;

    /// <summary>发送失败时的底层异常（SendRequestOk==false 时由调用方设置）。</summary>
    public Exception? Cause { get; set; }

    private readonly ManualResetEventSlim _event = new(false);

    private readonly object _cbLock = new();
    private bool _callbackFired;

    public RequestResponseFuture(string correlationId, int timeoutMillis,
        RequestCallback? requestCallback = null)
    {
        CorrelationId = correlationId;
        TimeoutMillis = timeoutMillis;
        RequestCallback = requestCallback;
        BeginTimestamp = UtilAll.CurrentTimeMillis();
    }

    // ---------- 等待 / 投递 ----------

    public Message? WaitResponseMessage(int timeoutMillis)
    {
        _event.Wait(Math.Max(0, timeoutMillis));
        return ResponseMsg;
    }

    public void PutResponseMessage(Message? responseMsg)
    {
        ResponseMsg = responseMsg;
        _event.Set();
    }

    public bool IsTimeout()
    {
        return UtilAll.CurrentTimeMillis() - BeginTimestamp > TimeoutMillis;
    }

    public void ExecuteRequestCallback()
    {
        if (RequestCallback is null)
        {
            return;
        }

        bool fire;
        lock (_cbLock)
        {
            fire = !_callbackFired;
            _callbackFired = true;
        }

        if (!fire)
        {
            return;
        }

        if (SendRequestOk && Cause is null)
        {
            RequestCallback.OnSuccess(ResponseMsg);
        }
        else
        {
            RequestCallback.OnException(Cause);
        }
    }
}

public sealed class RequestFutureHolder
{
    private readonly Dictionary<string, RequestResponseFuture> _table =
        new(StringComparer.Ordinal);

    private readonly object _lock = new();

    public static RequestFutureHolder Instance { get; } = new();

    public void PutRequest(string correlationId, RequestResponseFuture future)
    {
        lock (_lock)
        {
            _table[correlationId] = future;
        }
    }

    public RequestResponseFuture? GetRequest(string correlationId)
    {
        lock (_lock)
        {
            return _table.TryGetValue(correlationId, out RequestResponseFuture? f) ? f : null;
        }
    }

    public RequestResponseFuture? RemoveRequest(string correlationId)
    {
        lock (_lock)
        {
            return _table.Remove(correlationId, out RequestResponseFuture? f) ? f : null;
        }
    }

    /// 返回被填充的 future；查不到（已超时/已移除）时返回 null，调用方据此只记日志。
    /// </summary>
    public RequestResponseFuture? PutResponse(string correlationId, Message? responseMsg)
    {
        RequestResponseFuture? future = RemoveRequest(correlationId);
        if (future is null)
        {
            return null;
        }

        future.PutResponseMessage(responseMsg);
        future.ExecuteRequestCallback();
        return future;
    }


    private readonly HashSet<object> _producers = new();

    private readonly object _lifecycleLock = new();
    private Thread? _scanThread;
    private ManualResetEventSlim? _scanStop;

    public List<RequestResponseFuture> ScanExpiredRequest()
    {
        var expired = new List<RequestResponseFuture>();
        lock (_lock)
        {
            foreach (KeyValuePair<string, RequestResponseFuture> kv in _table)
            {
                if (!kv.Value.IsTimeout())
                {
                    continue;
                }

                if (_table.TryGetValue(kv.Key, out RequestResponseFuture? rf) && rf.IsTimeout())
                {
                    _table.Remove(kv.Key);
                    expired.Add(rf);
                    ClientLog.Warn("remove timeout request, CorrelationId=" + rf.CorrelationId);
                }
            }
        }

        foreach (RequestResponseFuture rf in expired)
        {
            try
            {
                rf.Cause = new RequestTimeoutException("request timeout, no reply message.");
                rf.ExecuteRequestCallback();
            }
            catch (Exception e)
            {
                ClientLog.Warn("scanResponseTable, operationComplete Exception: " + e.Message);
            }
        }

        return expired;
    }

    public void StartScheduledTask(object producer)
    {
        lock (_lifecycleLock)
        {
            _producers.Add(producer);
            if (_scanThread is not null)
            {
                return;
            }

            var stop = new ManualResetEventSlim(false);
            _scanStop = stop;
            var thread = new Thread(() =>
            {
                long next = Environment.TickCount64 + 3000;
                while (!stop.IsSet)
                {
                    Schedules.WaitUntil(stop, next);
                    if (stop.IsSet)
                    {
                        break;
                    }

                    next += 1000;
                    try
                    {
                        ScanExpiredRequest();
                    }
                    catch (Exception e)
                    {
                        ClientLog.Error("scan RequestFutureTable exception: " + e.Message);
                    }
                }
            })
            {
                IsBackground = true,
                Name = "RequestHouseKeepingService",
            };
            _scanThread = thread;
            thread.Start();
        }
    }

    public void ShutdownScheduledTask(object producer)
    {
        lock (_lifecycleLock)
        {
            _producers.Remove(producer);
            if (_producers.Count > 0 || _scanThread is null)
            {
                return;
            }

            Thread thread = _scanThread;
            ManualResetEventSlim stop = _scanStop!;
            _scanThread = null;
            _scanStop = null;
            stop.Set();
            thread.Join(500);
        }
    }
}

/// <summary>Request-Reply 工具函数（对应 Python request_reply.py 的模块级函数）。</summary>
public static class RequestReply
{
    public static string CreateCorrelationId() => Guid.NewGuid().ToString();

    public static Message CreateReplyMessage(Message requestMessage, byte[] body)
    {
        if (requestMessage is null)
        {
            throw new MQClientException("create reply message fail, requestMessage cannot be null.",
                ClientErrorCode.CreateReplyMessageException);
        }

        string cluster = requestMessage.GetProperty(MessageConst.PropertyCluster);
        if (string.IsNullOrEmpty(cluster))
        {
            throw new MQClientException(
                "create reply message fail, requestMessage error, property[" + MessageConst.PropertyCluster
                + "] is null.", ClientErrorCode.CreateReplyMessageException);
        }

        var reply = new Message
        {
            Topic = MixAll.GetReplyTopic(cluster),
            Body = body ?? Array.Empty<byte>(),
        };
        reply.PutProperty(MessageConst.PropertyMessageType, MixAll.REPLY_MESSAGE_FLAG);
        reply.PutProperty(MessageConst.PropertyCorrelationId,
            requestMessage.GetProperty(MessageConst.PropertyCorrelationId));
        reply.PutProperty(MessageConst.PropertyReplyToClient,
            requestMessage.GetProperty(MessageConst.PropertyReplyToClient));
        reply.PutProperty(MessageConst.PropertyMessageTTL,
            requestMessage.GetProperty(MessageConst.PropertyMessageTTL));
        return reply;
    }

    /// <summary>MSG_TYPE == "reply" 的发送要走 SEND_REPLY_MESSAGE_V2(325)。大小写敏感。</summary>
    public static bool IsReplyMessage(Message msg) =>
        msg.GetProperty(MessageConst.PropertyMessageType) == MixAll.REPLY_MESSAGE_FLAG;
}
