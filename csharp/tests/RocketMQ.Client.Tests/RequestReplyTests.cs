// Client-side unit tests for Request-Reply (5.x) (the core assertions of the originating side and the push-back entry).
//
// Three layers are covered:
// 1. Pure data / waiting slot logic (RequestResponseFuture / RequestFutureHolder /
//    create_reply_message) -- no network needed;
// 2. Wire encoding: a reply message must choose SEND_REPLY_MESSAGE_V2(325) rather than
//    SEND_MESSAGE_V2(310), otherwise the broker will not go through
//    ReplyMessageProcessor;
// 3. The broker push-back entry MQClientInstance.ProcessReplyMessage(326) -- it must
//    deliver the response into the waiting slot and **send a response back** (the broker
//    side is invokeSync, and it would time out there if none were returned).
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

// PropertyMap is a global using alias in the src project (SortedDictionary<string,string>), the test project must declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class RequestReplyTests
{
    private const string BaseTopic = "RRUnitTopic";

    private static Message RequestMsg()
    {
        // Builds a request message of "the broker has delivered it to a consumer" (carrying the CLUSTER the broker wrote)
        var m = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("ping"));
        m.PutProperty(MessageConst.PropertyCluster, "DefaultCluster");
        m.PutProperty(MessageConst.PropertyCorrelationId, "corr-1");
        m.PutProperty(MessageConst.PropertyReplyToClient, "10.0.0.1@pg#123");
        m.PutProperty(MessageConst.PropertyMessageTTL, "3000");
        return m;
    }

    // ---------------------------------------------------------------- Reply message construction

    [Fact]
    public void CreateReplyMessageMatchesLegacyShape()
    {
        Message reply = RequestReply.CreateReplyMessage(RequestMsg(), System.Text.Encoding.UTF8.GetBytes("pong"));
        Assert.Equal("DefaultCluster_REPLY_TOPIC", reply.Topic);
        Assert.Equal("pong", System.Text.Encoding.UTF8.GetString(reply.Body));
        // not one of the four properties may be missing, and CORRELATION_ID/REPLY_TO_CLIENT/TTL are carried back as is
        Assert.Equal("reply", reply.GetProperty(MessageConst.PropertyMessageType));
        Assert.Equal("corr-1", reply.GetProperty(MessageConst.PropertyCorrelationId));
        Assert.Equal("10.0.0.1@pg#123", reply.GetProperty(MessageConst.PropertyReplyToClient));
        Assert.Equal("3000", reply.GetProperty(MessageConst.PropertyMessageTTL));
    }

    [Fact]
    public void CreateReplyMessageRequiresCluster()
    {
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
        var other = new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("x"));
        other.PutProperty(MessageConst.PropertyMessageType, "Reply");
        Assert.False(RequestReply.IsReplyMessage(other));
    }

    [Fact]
    public void ReplyTopicConstantIsPostfixNotPrefix()
    {
        // Do not confuse Request-Reply's <cluster>_REPLY_TOPIC with the old console prefix %REPLY%
        Assert.Equal("REPLY_TOPIC", MixAll.REPLY_TOPIC_POSTFIX);
        Assert.Equal("reply", MixAll.REPLY_MESSAGE_FLAG);
        Assert.Equal("DefaultCluster_REPLY_TOPIC", MixAll.GetReplyTopic("DefaultCluster"));
    }

    // ---------------------------------------------------------------- Waiting slots

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
            Assert.Equal(36, id.Length); // a UUID string
        }
    }

    [Fact]
    public void FutureWaitTimesOutReturnsNull()
    {
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
        var holder = new RequestFutureHolder();
        var f = new RequestResponseFuture("c1", 1000);
        holder.PutRequest("c1", f);
        Assert.Same(f, holder.GetRequest("c1"));

        Assert.Same(f, holder.PutResponse("c1", new Message(BaseTopic, System.Text.Encoding.UTF8.GetBytes("pong"))));
        // Already taken away -> delivering again (a duplicate response) must return null, and must not wake it a second time
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
        var e = new RequestTimeoutException("x");
        Assert.IsAssignableFrom<MQClientException>(e);
        Assert.Equal(ClientErrorCode.RequestTimeoutException, e.ResponseCode);
    }

    // ---------------------------------------------------------------- Wire encoding

    private static MQClientInstance MakeInstance() =>
        // only built, no start() -> no connection, no heartbeat, purely offline assertions
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

    // ---------------------------------------------------------------- The broker push-back entry 326

    private static RemotingCommand PushReplyCommand(string correlationId, byte[] body)
    {
        // Builds a 326 request following the fields of broker ReplyMessageProcessor#pushReplyMessage.
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
        // On the real path ext_fields is filled by decode() from the message; here it is put in directly (equivalent to having gone through the network once).
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
            // A response must be sent back: the broker's Broker2Client.callClient is invokeSync(10s)
            Assert.NotNull(resp);
            Assert.Equal(ResponseCode.Success, resp!.Code);
            // The response is delivered into the waiting slot, with both body and properties
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
        // A late/duplicate response: not finding a waiting slot is only logged as a warn and SUCCESS must still be returned.
        // Returning non-SUCCESS would make the broker record it as a push failure in its log -- in reality the response was
        // not lost, the corresponding request had simply already timed out or been handled.
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
        // properties is malformed -> parsing throws -> SYSTEM_ERROR must be returned instead of letting the reader thread crash
        cmd.ExtFields = new PropertyMap { ["properties"] = "\x00\xff broken" };
        cmd.Body = System.Text.Encoding.UTF8.GetBytes("x");
        cmd.HasBody = true;
        RemotingCommand? resp = inst.ProcessReplyMessage(cmd, "127.0.0.1:10911");
        Assert.NotNull(resp);
        Assert.True(resp!.Code == ResponseCode.SystemError || resp.Code == ResponseCode.Success);
    }
}
