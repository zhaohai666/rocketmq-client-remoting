// POP + 顺序监听器（Java ConsumeMessagePopOrderlyService，5.5.0 未完成骨架）本地单测。
//
// Java DefaultMQPushConsumerImpl:960-990 按 listener 类型选服务：顺序监听器 + POP 走
// ConsumeMessagePopOrderlyService。上游 5.5.0 那是个未完成骨架（:533 POPTODO）：请求
// 去重入队后 run() 拿到队列锁就返回 —— 消息**不消费、不 ack**，invisibleTime 到期由
// broker 复活重投，宏观表现是「顺序 + POP 收不到消息且积压不消」。
//
// 覆盖：分派进顺序骨架（listener 不被调、不 ack）、并发分支回归护栏、去重集按
// (pq 引用, mq key) 判等、pq 撤销才摘请求、活队列 no-op。
// 与 cpp tests/test_pop_orderly.cpp、python tests/test_pop_orderly.py 逐条对应。
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PopOrderlyTests
{
    private const string Group = "GID_PopOrderlyDotNet";
    private const string Topic = "PopOrderlyDotNetTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Mq(int queueId = 0) => new MessageQueue(Topic, Broker, queueId);

    private static MessageExt Msg(long queueOffset, bool withCk = false)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            Body = System.Text.Encoding.UTF8.GetBytes("body-" + queueOffset),
            BrokerName = Broker,
            QueueId = 0,
            QueueOffset = queueOffset,
            MsgId = "pop-orderly-" + queueOffset.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (withCk)
        {
            // popTime 必须是"刚弹出"，否则 IsPopTimeout 会把本批按超时丢弃
            m.PutProperty(MessageConst.PropertyPopCk, ExtraInfoUtil.BuildExtraInfo(
                queueOffset, UtilAll.CurrentTimeMillis(), 60000, 0, Topic, Broker, 0, queueOffset));
        }
        return m;
    }

    private sealed class CountingOrderly : IMessageListenerOrderly
    {
        public int Calls;
        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
        {
            Calls++;
            return ConsumeOrderlyStatus.Success;
        }
    }

    private sealed class CountingConcurrent : IMessageListenerConcurrently
    {
        public int Calls;
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
        {
            Calls++;
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private static DefaultMQPushConsumer FreshConsumer(IMessageListener listener)
    {
        var c = new DefaultMQPushConsumer(Group)
        {
            PopMode = true,
        };
        c.SetMessageListener(listener);
        return c;
    }

    [Fact]
    public void OrderlyDispatch_NeverInvokesListener()
    {
        var listener = new CountingOrderly();
        var c = FreshConsumer(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(2);
        c.SubmitPopConsumeRequest(new List<MessageExt> { Msg(0), Msg(1) }, pq, Mq());
        Assert.Equal(0, listener.Calls);
        Assert.Equal(2, pq.WaitAckCount());
        Assert.Equal(1, c.PopOrderlyRequestCount());
    }

    [Fact]
    public void ConcurrentDispatch_StillConsumes()
    {
        var listener = new CountingConcurrent();
        var c = FreshConsumer(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(1);
        c.SubmitPopConsumeRequest(new List<MessageExt> { Msg(0, withCk: true) }, pq, Mq());
        Assert.Equal(1, listener.Calls);
        Assert.Equal(0, pq.WaitAckCount());
    }

    [Fact]
    public void RequestDedup_ByPqReferenceAndMqKey()
    {
        var c = FreshConsumer(new CountingOrderly());
        var pq = new PopProcessQueue();
        c.SubmitPopOrderlyRequest(pq, Mq(), force: false);
        c.SubmitPopOrderlyRequest(pq, Mq(), force: false);
        Assert.Equal(1, c.PopOrderlyRequestCount());
        c.SubmitPopOrderlyRequest(pq, Mq(1), force: false);
        Assert.Equal(2, c.PopOrderlyRequestCount());
        c.SubmitPopOrderlyRequest(new PopProcessQueue(), Mq(), force: false);
        Assert.Equal(3, c.PopOrderlyRequestCount());
        c.SubmitPopOrderlyRequest(pq, Mq(), force: true);
        Assert.Equal(3, c.PopOrderlyRequestCount());
    }

    [Fact]
    public void DroppedRequest_IsRemoved()
    {
        var listener = new CountingOrderly();
        var c = FreshConsumer(listener);
        var pq = new PopProcessQueue();
        c.SubmitPopOrderlyRequest(pq, Mq(), force: false);
        Assert.Equal(1, c.PopOrderlyRequestCount());
        pq.SetDropped(true);
        c.RunPopOrderlyRequest(pq, Mq());
        Assert.Equal(0, c.PopOrderlyRequestCount());
        Assert.Equal(0, listener.Calls);
        c.SubmitPopOrderlyRequest(pq, Mq(), force: false);
        Assert.Equal(0, c.PopOrderlyRequestCount());
    }

    [Fact]
    public void RunOnLiveQueue_IsNoOp()
    {
        var listener = new CountingOrderly();
        var c = FreshConsumer(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(3);
        c.SubmitPopOrderlyRequest(pq, Mq(), force: false);
        c.RunPopOrderlyRequest(pq, Mq());
        Assert.Equal(1, c.PopOrderlyRequestCount());
        Assert.Equal(3, pq.WaitAckCount());
        Assert.Equal(0, listener.Calls);
    }
}
