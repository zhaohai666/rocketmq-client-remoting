// Request-Reply（5.x）客户端侧支撑（对应 org.apache.rocketmq.client.producer 的
// RequestResponseFuture / RequestFutureHolder / MessageUtil#createReplyMessage 与
// org.apache.rocketmq.client.impl.ClientRemotingProcessor#receiveReplyMessage）。
//
// 协议回顾（照 Java 逐字段复刻）：
//   请求方 (producer.Request)                       应答方 (push consumer)
//   ──────────────────────────                      ─────────────────────
//   CORRELATION_ID = uuid
//   REPLY_TO_CLIENT = clientId ──────────────►      收到请求消息（broker 已写 CLUSTER）
//   TTL            = timeoutMillis                  create_reply_message(req, body):
//                                                    topic     = <CLUSTER>_REPLY_TOPIC
//                                                    CORRELATION_ID/REPLY_TO_CLIENT/TTL 原样带回
//                                                    MSG_TYPE  = "reply"
//   ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ──────    producer.send(reply) →
//       （broker 按 REPLY_TO_CLIENT 找到请求方连接）   broker 走 SEND_REPLY_MESSAGE_V2(325)
//
// 两个关键点（错了真机就不通）：
// 1. 应答消息必须带 MSG_TYPE == "reply"，发送时据此把请求码从 SEND_MESSAGE_V2(310)
//    换成 SEND_REPLY_MESSAGE_V2(325)；broker 的 ReplyMessageProcessor 只在 324/325 注册。
// 2. REPLY_TO_CLIENT 是请求方的 clientId，broker 靠它在 producerManager 里反查 channel
//    才能把应答推回——所以请求方必须先发过心跳（已注册为 producer）。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>
/// 对应 Java RequestTimeoutException：请求已发出但超时没收到应答。必须继承
/// MQClientException（Java 中 RequestTimeoutException extends MQClientException）。
///
/// Java 抛的是 <c>RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, msg)</c>：
/// 光有类型不够，10006 这个码也要带上 —— 调用方按 ResponseCode 分流时才知道
/// "消息已经投出去了，只是没等到应答"（对方可能只是慢），这跟发送本身失败是两类处置。
/// </summary>
public class RequestTimeoutException : MQClientException
{
    public RequestTimeoutException(string msg)
        : base(msg, ClientErrorCode.RequestTimeoutException)
    {
    }
}

/// <summary>对应 Java RequestCallback（一次 request 的异步回调）。</summary>
public abstract class RequestCallback
{
    public abstract void OnSuccess(Message? responseMessage);

    public abstract void OnException(Exception? e);
}

/// <summary>
/// 对应 Java RequestResponseFuture：一次 request 的等待槽。
///
/// 超时的**兜底清理**由 <see cref="RequestFutureHolder.ScanExpiredRequest"/> 的周期扫描负责
/// （Java scanExpiredRequest 同款）：等待槽到期还没人投应答时被摘除并触发一次回调。
/// </summary>
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

    /// <summary>对应 Java waitResponseMessage：等 latch（超时返回 null）。</summary>
    public Message? WaitResponseMessage(int timeoutMillis)
    {
        _event.Wait(Math.Max(0, timeoutMillis));
        return ResponseMsg;
    }

    /// <summary>对应 Java putResponseMessage（会 set latch，可多次调用）。</summary>
    public void PutResponseMessage(Message? responseMsg)
    {
        ResponseMsg = responseMsg;
        _event.Set();
    }

    public bool IsTimeout()
    {
        return UtilAll.CurrentTimeMillis() - BeginTimestamp > TimeoutMillis;
    }

    /// <summary>对应 Java executeRequestCallback：回调只允许触发一次。</summary>
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

/// <summary>
/// 对应 Java RequestFutureHolder：correlationId → 等待槽 的全局表。
///
/// Java 里是跨 producer 共享的单例（RequestFutureHolder.getInstance()），因为应答由
/// clientId 级别的 remoting 通道推回，与具体 producer 实例无关。
/// </summary>
public sealed class RequestFutureHolder
{
    private readonly Dictionary<string, RequestResponseFuture> _table =
        new(StringComparer.Ordinal);

    private readonly object _lock = new();

    /// <summary>进程内单例（对齐 Java 的 INSTANCE）。应答方与请求方在同一进程时也共用它。</summary>
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

    /// <summary>
    /// 接收侧入口（对齐 Java processReplyMessage）：投递应答。
    ///
    /// Java 在这里做的是 getRequestFutureTable().remove(correlationId)——用「谁摘到谁负责」
    /// 保证「应答到达」与「超时清理」两条路径只会有一个生效。本实现照搬该语义。
    ///
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
        // 对齐 Java：成功路径也走 executeRequestCallback，让「只回调一次」的守卫生效；
        // 同步调用方（callback 为空）靠 PutResponseMessage 唤醒。
        future.ExecuteRequestCallback();
        return future;
    }

    // ---------- 超时兜底扫描（Java scanExpiredRequest / startScheduledTask / shutdown） ----------

    /// <summary>引用计数用的 producer 集合（对齐 Java producerSet：按实例去重）。</summary>
    private readonly HashSet<object> _producers = new();

    private readonly object _lifecycleLock = new();
    private Thread? _scanThread;
    private ManualResetEventSlim? _scanStop;

    /// <summary>
    /// 对应 Java scanExpiredRequest：摘除所有已到期的等待槽并触发一次回调。
    ///
    /// 两个要点照搬 Java：
    /// 1. 先「摘除」再「回调」，两步分开 —— 回调里如果又发起 request（重试场景）不会死锁；
    /// 2. remove-claims-ownership：只有真正从表里摘掉的那个线程有权触发回调，这样
    ///    「应答到达（PutResponse）」与「超时清理」两条路径只有一条生效。
    ///
    /// 同步调用（callback 为 null）不在此唤醒：它在 WaitResponseMessage 的 latch 上
    /// 自行超时，与 Java 行为一致。返回本轮摘除的 future（测试断言用）。
    /// </summary>
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

                // 再次校验 isTimeout 后才摘（对齐 Java 的迭代器 remove：只摘扫到的那条，
                // 防止同一 correlationId 被重新 put 后误伤新槽）。
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

    /// <summary>
    /// 对应 Java startScheduledTask(producer)：把 producer 计入引用集合，并懒启动
    /// 扫描线程（线程名 RequestHouseKeepingService；首扫延迟 3000ms、周期 1000ms，
    /// scheduleAtFixedRate 语义 —— 计划时刻锚定，落后即刻补跑）。
    /// 单例表被多个 producer 共享，任一存活就得继续扫。
    /// </summary>
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

    /// <summary>
    /// 对应 Java shutdown(producer)：仅当最后一个 producer 退场时才停扫描线程。
    /// </summary>
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
    /// <summary>对应 Java CorrelationIdUtil.createCorrelationId（随机 UUID 字符串）。</summary>
    public static string CreateCorrelationId() => Guid.NewGuid().ToString();

    /// <summary>
    /// 对应 Java MessageUtil.createReplyMessage：由请求消息派生出应答消息。
    ///
    /// CLUSTER 属性由 broker 在投递时写入（SendMessageProcessor），拿不到就说明这条消息
    /// 不是经 broker 转发过来的（或 topic 配得不对），与 Java 一样直接抛错，而不是造一条
    /// 投不出去的应答。
    ///
    /// Java 抛的是 <c>MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION, 同样的文案)</c>
    /// （<c>MessageUtil:46/49</c>）：应答方通常写在业务 listener 里，按 ResponseCode 分流
    /// 才能把"这条请求不能回话"和别的本地错误分开。
    /// </summary>
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
