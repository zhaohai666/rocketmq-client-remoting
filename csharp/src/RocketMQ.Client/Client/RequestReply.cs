// Two key points (get them wrong and it won't work on a real cluster):
// 1. The reply message must carry MSG_TYPE == "reply"; at send time this switches the request code from SEND_MESSAGE_V2(310)
//    to SEND_REPLY_MESSAGE_V2(325); the broker's ReplyMessageProcessor only registers on 324/325.
// 2. REPLY_TO_CLIENT is the requester's clientId; the broker uses it to look up the channel in producerManager
//    to push the reply back — so the requester must have sent a heartbeat first (already registered as a producer).
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

    /// <summary>Whether the send succeeded (the failure path sets it to false, to distinguish timeout vs send failure).</summary>
    public bool SendRequestOk { get; set; } = true;

    /// <summary>The underlying exception on send failure (set by the caller when SendRequestOk==false).</summary>
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

    // ---------- Wait / deliver ----------

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

    /// Returns the filled future; when not found (already timed out / removed) returns null, and the caller only logs based on that.
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

/// <summary>Request-Reply module-level helper functions.</summary>
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

    /// <summary>Sends with MSG_TYPE == "reply" must go through SEND_REPLY_MESSAGE_V2(325). Case-sensitive.</summary>
    public static bool IsReplyMessage(Message msg) =>
        msg.GetProperty(MessageConst.PropertyMessageType) == MixAll.REPLY_MESSAGE_FLAG;
}
