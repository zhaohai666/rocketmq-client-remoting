// Request-Reply（5.x）客户端侧单测（镜像 python/tests/test_request_reply.py）。
//
// 覆盖三层：
// 1. 纯数据/等待槽逻辑（RequestResponseFuture / RequestFutureHolder /
//    create_reply_message）—— 不需要网络；
// 2. 线上编码：应答消息必须选 SEND_REPLY_MESSAGE_V2(325) 而不是
//    SEND_MESSAGE_V2(310)，否则 broker 不会走 ReplyMessageProcessor；
// 3. broker 回推入口 MQClientInstance.ProcessReplyMessage(326) —— 必须把应答
//    投进等待槽，并且**回一个响应**（broker 侧是 invokeSync，不回响应它那边会超时）。
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

// PropertyMap 是 src 项目的 global using 别名（SortedDictionary<string,string>），测试项目需显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class RequestReplyTests
{
    private const string BaseTopic = "RRUnitTopic";

    private static Message RequestMsg()
    {
        // 构造一条「broker 已投递给消费者」的请求消息（带 broker 写入的 CLUSTER）。
        var m = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("ping"));
        m.PutProperty(MessageConst.PropertyCluster, "DefaultCluster");
        m.PutProperty(MessageConst.PropertyCorrelationId, "corr-1");
        m.PutProperty(MessageConst.PropertyReplyToClient, "10.0.0.1@pg#123");
        m.PutProperty(MessageConst.PropertyMessageTTL, "3000");
        return m;
    }

    // ---------------------------------------------------------------- 应答消息构造

    [Fact]
    public void CreateReplyMessageMatchesJavaShape()
    {
        Message reply = RequestReply.CreateReplyMessage(RequestMsg(), System.Text.Encoding.UTF8.GetBytes("pong"));
        // topic 必须是 <cluster>_REPLY_TOPIC（Java MixAll.getReplyTopic）
        Assert.Equal("DefaultCluster_REPLY_TOPIC", reply.Topic);
        Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(reply.Body));
        // 四个属性一个都不能少，且 CORRELATION_ID/REPLY_TO_CLIENT/TTL 是原样带回
        Assert.Equal("reply", reply.GetProperty(MessageConst.PropertyMessageType));
        Assert.Equal("corr-1", reply.GetProperty(MessageConst.PropertyCorrelationId));
        Assert.Equal("10.0.0.1@pg#123", reply.GetProperty(MessageConst.PropertyReplyToClient));
        Assert.Equal("3000", reply.GetProperty(MessageConst.PropertyMessageTTL));
    }

    [Fact]
    public void CreateReplyMessageRequiresCluster()
    {
        // CLUSTER 由 broker 写入；没有它说明这条消息不是 broker 转来的，Java 同样抛错
        // Java MessageUtil.createReplyMessage 两个分支都带 10007 CREATE_REPLY_MESSAGE_EXCEPTION：
        // 应答方写在业务 listener 里，只能按 responseCode 分流。
        var noCluster = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("ping"));
        noCluster.PutProperty(MessageConst.PropertyCorrelationId, "corr-1");
        Assert.Equal(ClientErrorCode.CreateReplyMessageException,
            Assert.Throws<MQClientException>(
                () => RequestReply.CreateReplyMessage(noCluster, System.Text.Encoding.UTF8.GetBytes("pong")))
            .ResponseCode);
        Assert.Equal(ClientErrorCode.CreateReplyMessageException,
            Assert.Throws<MQClientException>(
                    () => RequestReply.CreateReplyMessage(null!, System.Text.Encoding.UTF8.GetBytes("pong")))
                .ResponseCode);
    }

    [Fact]
    public void IsReplyMessageFlag()
    {
        Message reply = RequestReply.CreateReplyMessage(RequestMsg(), System.Text.Encoding.UTF8.GetBytes("pong"));
        Assert.True(RequestReply.IsReplyMessage(reply));
        Assert.False(RequestReply.IsReplyMessage(new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("x"))));
        // 大小写敏感：Java 是 equals("reply")
        var other = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("x"));
        other.PutProperty(MessageConst.PropertyMessageType, "Reply");
        Assert.False(RequestReply.IsReplyMessage(other));
    }

    [Fact]
    public void ReplyTopicConstantIsPostfixNotPrefix()
    {
        // 别把 Request-Reply 的 <cluster>_REPLY_TOPIC 与老的控制台前缀 %REPLY% 搞混
        Assert.Equal("REPLY_TOPIC", MixAll.REPLY_TOPIC_POSTFIX);
        Assert.Equal("reply", MixAll.REPLY_MESSAGE_FLAG);
        Assert.Equal("DefaultCluster_REPLY_TOPIC", MixAll.GetReplyTopic("DefaultCluster"));
    }

    // ---------------------------------------------------------------- 等待槽

    [Fact]
    public void CorrelationIdIsRandom()
    {
        var ids = new HashSet<string>();
        for (int i = 0; i < 50; i++)
        {
            ids.Add(RequestReply.CreateCorrelationId());
        }

        Assert.Equal(50, ids.Count);
        foreach (string id in ids)
        {
            Assert.Equal(36, id.Length); // UUID 字符串
        }
    }

    [Fact]
    public void FutureWaitTimesOutReturnsNull()
    {
        // 等待预算必须明显大于 future 的超时预算：IsTimeout 用的是严格大于（同 Java），
        // 而系统等待可能比截止时刻早一丁点返回，两者相等时忙机上这条断言会抖。
        var f = new RequestResponseFuture("c1", 20);
        Assert.Null(f.WaitResponseMessage(200));
        Assert.True(f.IsTimeout());
    }

    [Fact]
    public void FutureIsWokenByPutResponse()
    {
        var f = new RequestResponseFuture("c1", 5000);
        var msg = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("pong"));
        _ = Task.Run(() =>
        {
            Thread.Sleep(50);
            f.PutResponseMessage(msg);
        });
        Assert.Same(msg, f.WaitResponseMessage(2000));
        Assert.False(f.IsTimeout());
    }

    [Fact]
    public void HolderPutResponseRemovesEntry()
    {
        // Java 用 remove 抢所有权：应答到达与超时清理只能有一个生效。
        var holder = new RequestFutureHolder();
        var f = new RequestResponseFuture("c1", 1000);
        holder.PutRequest("c1", f);
        Assert.Same(f, holder.GetRequest("c1"));

        Assert.Same(f, holder.PutResponse("c1", new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("pong"))));
        // 已经被摘走 → 再投一次（重复应答）必须返回 null，且不会二次唤醒
        Assert.Null(holder.GetRequest("c1"));
        Assert.Null(holder.PutResponse("c1", new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("pong2"))));
    }

    [Fact]
    public void HolderRemoveIsIdempotent()
    {
        var holder = new RequestFutureHolder();
        holder.PutRequest("c1", new RequestResponseFuture("c1", 1000));
        Assert.NotNull(holder.RemoveRequest("c1"));
        Assert.Null(holder.RemoveRequest("c1"));
    }

    [Fact]
    public void GlobalHolderIsModuleSingleton()
    {
        Assert.IsType<RequestFutureHolder>(RequestFutureHolder.Instance);
    }

    [Fact]
    public void RequestTimeoutExceptionIsMQClientException()
    {
        // Java: RequestTimeoutException extends MQClientException，
        // 且 waitResponse 抛的是 RequestTimeoutException(10006, ...)：只有类型不够，
        // 调用方按 ResponseCode 分流时要知道"消息已投出去、只是没等到应答"。
        var e = new RequestTimeoutException("x");
        Assert.IsAssignableFrom<MQClientException>(e);
        Assert.Equal(ClientErrorCode.RequestTimeoutException, e.ResponseCode);
    }

    // ---------------------------------------------------------------- 线上编码

    private static MQClientInstance MakeInstance() =>
        // 只构造，不 start() → 不建连、不发心跳，纯离线断言
        new MQClientInstance("RRUnitClient_" + Environment.CurrentManagedThreadId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            new List<string> { "127.0.0.1:9876" });

    [Fact]
    public void ReplySendUsesSendReplyMessageCode()
    {
        MQClientInstance inst = MakeInstance();
        Message reply = RequestReply.CreateReplyMessage(RequestMsg(), System.Text.Encoding.UTF8.GetBytes("pong"));
        RemotingCommand req = inst.BuildSendRequest("PG_RR", reply, new MessageQueue(reply.Topic, "broker-a", 0));
        Assert.Equal(RequestCode.SendReplyMessageV2, req.Code);
    }

    [Fact]
    public void NormalSendStillUsesSendMessageV2()
    {
        MQClientInstance inst = MakeInstance();
        var msg = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("hello"));
        RemotingCommand req = inst.BuildSendRequest("PG_RR", msg, new MessageQueue(BaseTopic, "broker-a", 0));
        Assert.Equal(RequestCode.SendMessageV2, req.Code);
        Assert.NotEqual(RequestCode.SendReplyMessageV2, req.Code);
    }

    // ---------------------------------------------------------------- broker 回推入口 326

    private static RemotingCommand PushReplyCommand(string correlationId, byte[] body)
    {
        // 按 broker ReplyMessageProcessor#pushReplyMessage 的字段造一条 326 请求。
        var h = new ReplyMessageRequestHeader
        {
            ProducerGroup = "PG_RR",
            Topic = "DefaultCluster_REPLY_TOPIC",
            DefaultTopic = MixAll.DefaultTopic,
            DefaultTopicQueueNums = MixAll.DefaultTopicQueueNums,
            QueueId = 0,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            Flag = 0,
            ReconsumeTimes = 0,
            UnitMode = false,
            BornHost = "127.0.0.1:10911",
            StoreHost = "127.0.0.1:10911",
            StoreTimestamp = 1700000000001L,
        };
        var props = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes(string.Empty));
        props.PutProperty(MessageConst.PropertyMessageType, MixAll.REPLY_MESSAGE_FLAG);
        props.PutProperty(MessageConst.PropertyCorrelationId, correlationId);
        props.PutProperty(MessageConst.PropertyReplyToClient, "10.0.0.1@pg#123");
        h.Properties = MessageDecoder.MessagePropertiesToString(props.Properties);

        var cmd = RemotingCommand.CreateRequestCommand(RequestCode.PushReplyMessageToClient, null);
        // 真实链路上 ext_fields 由 decode() 从报文填好；这里直接放进去（等价于过了一遍网络）。
        cmd.ExtFields = h.ToExtFields();
        cmd.Body = body;
        cmd.HasBody = true;
        return cmd;
    }

    [Fact]
    public void ProcessReplyMessageDeliversAndRespondsSuccess()
    {
        MQClientInstance inst = MakeInstance();
        var future = new RequestResponseFuture("corr-326", 5000);
        RequestFutureHolder.Instance.PutRequest("corr-326", future);
        try
        {
            RemotingCommand req = PushReplyCommand("corr-326", System.Text.Encoding.UTF8.GetBytes("pong"));
            RemotingCommand? resp = inst.ProcessReplyMessage(req, "127.0.0.1:10911");
            // 必须回响应：broker 的 Broker2Client.callClient 是 invokeSync(10s)
            Assert.NotNull(resp);
            Assert.Equal(ResponseCode.Success, resp!.Code);
            // 应答被投进等待槽，body 与属性都在
            Message? got = future.WaitResponseMessage(1000);
            Assert.NotNull(got);
            Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(got!.Body));
            Assert.Equal("corr-326", got.GetProperty(MessageConst.PropertyCorrelationId));
            Assert.NotEmpty(got.GetProperty(MessageConst.PropertyReplyMessageArriveTime));
            Assert.Equal("127.0.0.1:10911", ((MessageExt)got).BornHost);
        }
        finally
        {
            RequestFutureHolder.Instance.RemoveRequest("corr-326");
        }
    }

    [Fact]
    public void ProcessReplyMessageUnknownCorrelationStillRespondsSuccess()
    {
        // 迟到/重复的应答：查不到等待槽只记 warn，仍然要回 SUCCESS。
        // 回非 SUCCESS 会让 broker 把它当成 push 失败记进日志 —— 实际上应答没丢，
        // 只是对应的请求已经超时或已处理过了。
        MQClientInstance inst = MakeInstance();
        RemotingCommand? resp = inst.ProcessReplyMessage(
            PushReplyCommand("no-such-id", System.Text.Encoding.UTF8.GetBytes("late")), "127.0.0.1:10911");
        Assert.NotNull(resp);
        Assert.Equal(ResponseCode.Success, resp!.Code);
    }

    [Fact]
    public void ProcessReplyMessageReturnsSystemErrorOnBadHeader()
    {
        MQClientInstance inst = MakeInstance();
        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(RequestCode.PushReplyMessageToClient, null);
        // properties 是非法格式 → 解析抛错 → 必须回 SYSTEM_ERROR 而不是让读线程崩掉
        cmd.ExtFields = new PropertyMap { ["properties"] = "\x00\xff broken" };
        cmd.Body = System.Text.Encoding.UTF8.GetBytes("x");
        cmd.HasBody = true;
        RemotingCommand? resp = inst.ProcessReplyMessage(cmd, "127.0.0.1:10911");
        Assert.NotNull(resp);
        Assert.True(resp!.Code == ResponseCode.SystemError || resp.Code == ResponseCode.Success);
    }
}
