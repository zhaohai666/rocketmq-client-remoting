// 钩子单测：CheckForbiddenHook（发送前拦截）与 FilterMessageHook（投递前过滤）。
using System;
using System.Collections.Generic;

using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class HookTests
{
    private const string Topic = "TopicHook";
    private const string Group = "GID_HookUnit";

    private static MessageExt MakeMsg(string msgId, string body, string tags = "")
    {
        var m = new MessageExt
        {
            Topic = Topic,
            MsgId = msgId,
            Body = System.Text.Encoding.UTF8.GetBytes(body),
        };
        if (tags.Length > 0)
        {
            m.Properties[MessageConst.PropertyTags] = tags;
        }

        return m;
    }

    private static Message MakeMessage(string body) =>
        new(Topic, System.Text.Encoding.UTF8.GetBytes(body));

    private static string BodyOf(MessageExt m) => System.Text.Encoding.UTF8.GetString(m.Body);

    // ---- CheckForbiddenHook：记录调用与上下文，可选抛异常 ----
    private sealed class ForbidHook : ICheckForbiddenHook
    {
        private readonly bool _forbid;

        public ForbidHook(bool forbid) => _forbid = forbid;

        public int Calls { get; private set; }

        public CommunicationMode? LastMode { get; private set; }

        public string? LastGroup { get; private set; }

        public string? LastTopic { get; private set; }

        public bool LastUnitMode { get; private set; } = true;

        public bool LastSendResultNull { get; private set; }

        public bool LastArgNull { get; private set; } = true;

        public string? LastBody { get; private set; }

        public string HookName() => "unit-forbid";

        public void CheckForbidden(CheckForbiddenContext context)
        {
            Calls++;
            LastMode = context.CommunicationMode;
            LastGroup = context.Group;
            LastTopic = context.Mq?.Topic;
            LastUnitMode = context.UnitMode;
            LastSendResultNull = context.SendResult is null;
            LastArgNull = context.Arg is null;
            LastBody = context.Message is null
                ? null
                : System.Text.Encoding.UTF8.GetString(context.Message.Body);
            if (_forbid)
            {
                throw new MQClientException("forbidden by unit hook");
            }
        }
    }

    // ---- FilterMessageHook：按 body 前缀丢消息 ----
    private sealed class DropHook : IFilterMessageHook
    {
        private readonly string _prefix;

        public DropHook(string prefix) => _prefix = prefix;

        public int Calls { get; private set; }

        public List<int> SeenCounts { get; } = new();

        public string? SeenGroup { get; private set; }

        public bool SeenUnitMode { get; private set; } = true;

        public string HookName() => "unit-drop";

        public void FilterMessage(FilterMessageContext context)
        {
            Calls++;
            SeenCounts.Add(context.MsgList.Count);
            SeenGroup = context.ConsumerGroup;
            SeenUnitMode = context.UnitMode;
            var kept = new List<MessageExt>();
            foreach (MessageExt m in context.MsgList)
            {
                if (!BodyOf(m).StartsWith(_prefix, StringComparison.Ordinal))
                {
                    kept.Add(m);
                }
            }

            context.MsgList = kept;
        }
    }

    private sealed class BoomHook : IFilterMessageHook
    {
        public int Calls { get; private set; }

        public string HookName() => "unit-boom";

        public void FilterMessage(FilterMessageContext context)
        {
            Calls++;
            throw new InvalidOperationException("boom from unit hook");
        }
    }

    // ---------------------------------------------------------------- CheckForbiddenHook
    [Fact]
    public void CheckForbiddenHook_RegistryAndContext()
    {
        var producer = new DefaultMQProducer("GID_HookUnitProducer");
        Assert.False(producer.HasCheckForbiddenHook());
        Assert.Equal(0, producer.CheckForbiddenHookCount());
        Assert.False(producer.HasSendInterceptors());

        var hook = new ForbidHook(forbid: false);
        producer.RegisterCheckForbiddenHook(hook);
        Assert.True(producer.HasCheckForbiddenHook());
        Assert.Equal(1, producer.CheckForbiddenHookCount());
        Assert.True(producer.HasSendInterceptors());

        producer.RegisterCheckForbiddenHook(null!);
        Assert.Equal(1, producer.CheckForbiddenHookCount());

        var msg = MakeMessage("hello");
        var mq = new MessageQueue(Topic, "broker-a", 3);
        var ctx = new CheckForbiddenContext
        {
            NameSrvAddr = "127.0.0.1:9876",
            Group = "GID_HookUnitProducer",
            Message = msg,
            Mq = mq,
            BrokerAddr = "127.0.0.1:10911",
            CommunicationMode = CommunicationMode.Async,
            Arg = "order-key",
            UnitMode = false,
        };

        producer.ExecuteCheckForbiddenHook(ctx);
        Assert.Equal(1, hook.Calls);
        Assert.Equal(Topic, hook.LastTopic);
        Assert.Equal(CommunicationMode.Async, hook.LastMode);
        Assert.False(hook.LastUnitMode);
        Assert.True(hook.LastSendResultNull);
        Assert.False(hook.LastArgNull);
        Assert.Equal("hello", hook.LastBody);
    }

    [Fact]
    public void CheckForbiddenHook_ExceptionIsNotSwallowed()
    {
        var producer = new DefaultMQProducer("GID_HookUnitBlocker");
        var strict = new ForbidHook(forbid: true);
        var after = new ForbidHook(forbid: false);
        producer.RegisterCheckForbiddenHook(strict);
        producer.RegisterCheckForbiddenHook(after);

        var ctx = new CheckForbiddenContext
        {
            Group = "GID_HookUnitBlocker",
            Message = MakeMessage("x"),
            Mq = new MessageQueue(Topic, "broker-a", 0),
        };

        MQClientException ex = Assert.Throws<MQClientException>(
            () => producer.ExecuteCheckForbiddenHook(ctx));
        Assert.Equal("forbidden by unit hook", ex.Message);
        Assert.Equal(1, strict.Calls);
        // 异常抛出后，**后面的钩子不再执行** —— 这正是"拦截"的语义（发送已被中止）
        Assert.Equal(0, after.Calls);
    }

    [Fact]
    public void CheckForbiddenHook_AllHooksRunWhenNoneThrow()
    {
        var producer = new DefaultMQProducer("GID_HookUnitMulti");
        var a = new ForbidHook(forbid: false);
        var b = new ForbidHook(forbid: false);
        producer.RegisterCheckForbiddenHook(a);
        producer.RegisterCheckForbiddenHook(b);
        producer.ExecuteCheckForbiddenHook(new CheckForbiddenContext());
        Assert.Equal(1, a.Calls);
        Assert.Equal(1, b.Calls);
    }

    [Fact]
    public void CheckForbiddenHook_DefaultsMatchLegacy()
    {
        var ctx = new CheckForbiddenContext();
        Assert.Null(ctx.SendResult);
        Assert.False(ctx.UnitMode);
        Assert.Null(ctx.Arg);
        Assert.Equal(CommunicationMode.Sync, ctx.CommunicationMode);
        Assert.Null(ctx.Exception);

        Assert.Equal(0, (int)CommunicationMode.Sync);
        Assert.Equal(1, (int)CommunicationMode.Async);
        Assert.Equal(2, (int)CommunicationMode.Oneway);
    }

    // ---------------------------------------------------------------- FilterMessageHook
    [Fact]
    public void FilterMessageHook_DropsMessages()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        Assert.False(consumer.HasFilterMessageHook());
        Assert.Equal(0, consumer.FilterMessageHookCount());
        Assert.Equal(0, consumer.FilteredMessageCount());

        var drop = new DropHook("drop-");
        consumer.RegisterFilterMessageHook(drop);
        Assert.True(consumer.HasFilterMessageHook());
        Assert.Equal(1, consumer.FilterMessageHookCount());

        var mq = new MessageQueue(Topic, "broker-a", 0);
        var msgs = new List<MessageExt>
        {
            MakeMsg("M1", "keep-1", "TagA"),
            MakeMsg("M2", "drop-2", "TagA"),
            MakeMsg("M3", "keep-3", "TagA"),
        };

        List<MessageExt> kept = consumer.FilterMessagesForDelivery(mq, null, msgs);
        Assert.Equal(2, kept.Count);
        Assert.Equal("M1", kept[0].MsgId);
        Assert.Equal("M3", kept[1].MsgId);
        Assert.Single(drop.SeenCounts);
        Assert.Equal(3, drop.SeenCounts[0]);
        Assert.Equal(Group, drop.SeenGroup);
        Assert.False(drop.SeenUnitMode);
    }

    [Fact]
    public void FilterMessageHook_TagFilterRunsBeforeHook()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        var drop = new DropHook("drop-");
        consumer.RegisterFilterMessageHook(drop);

        var mq = new MessageQueue(Topic, "broker-a", 0);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(Topic, "TagA");
        var mixed = new List<MessageExt>
        {
            MakeMsg("M1", "keep-1", "TagA"),
            MakeMsg("M2", "keep-2", "TagB"),
            MakeMsg("M3", "drop-3", "TagA"),
        };

        List<MessageExt> kept = consumer.FilterMessagesForDelivery(mq, sub, mixed);
        Assert.Single(kept);
        Assert.Equal("M1", kept[0].MsgId);
        // 钩子只看到 tag 匹配后的 2 条 —— 证明 tag 过滤在钩子之前
        Assert.Single(drop.SeenCounts);
        Assert.Equal(2, drop.SeenCounts[0]);
    }

    [Fact]
    public void FilterMessageHook_SubAllDoesNotClientFilterTags()
    {
        var consumer = new DefaultMQPushConsumer(Group + "All");
        var noop = new DropHook("nothing-matches-this-");
        consumer.RegisterFilterMessageHook(noop);

        SubscriptionData subAll = FilterAPI.BuildSubscriptionData(Topic, "*");
        Assert.Empty(subAll.TagsSet);

        var mq = new MessageQueue(Topic, "broker-a", 0);
        var mixed = new List<MessageExt>
        {
            MakeMsg("M1", "keep-1", "TagA"),
            MakeMsg("M2", "keep-2", "TagB"),
        };

        List<MessageExt> kept = consumer.FilterMessagesForDelivery(mq, subAll, mixed);
        Assert.Equal(2, kept.Count);
    }

    [Fact]
    public void FilterMessageHook_ExceptionIsSwallowed()
    {
        // 与 CheckForbiddenHook 相反：过滤钩子的异常被吞掉，且**后续钩子照常执行**
        var consumer = new DefaultMQPushConsumer(Group + "Boom");
        var boom = new BoomHook();
        var drop = new DropHook("drop-");
        consumer.RegisterFilterMessageHook(boom);
        consumer.RegisterFilterMessageHook(drop);

        var mq = new MessageQueue(Topic, "broker-a", 0);
        var msgs = new List<MessageExt>
        {
            MakeMsg("M1", "keep-1"),
            MakeMsg("M2", "drop-2"),
        };

        List<MessageExt> kept = consumer.FilterMessagesForDelivery(mq, null, msgs);
        Assert.Equal(1, boom.Calls);
        Assert.Equal(1, drop.Calls);
        Assert.Single(kept);
    }

    [Fact]
    public void FilterMessageHook_EmptyInputShortCircuits()
    {
        var consumer = new DefaultMQPushConsumer(Group + "Empty");
        var drop = new DropHook("drop-");
        consumer.RegisterFilterMessageHook(drop);
        List<MessageExt> kept = consumer.FilterMessagesForDelivery(
            new MessageQueue(Topic, "broker-a", 0), null, new List<MessageExt>());
        Assert.Empty(kept);
        Assert.Equal(0, drop.Calls);
    }

    [Fact]
    public void DroppedMessages_ComputesDifference()
    {
        // POP 路径要给被摘掉的消息补 ack，因此差集必须精确
        var a = MakeMsg("A", "keep");
        var b = MakeMsg("B", "drop");
        var c = MakeMsg("C", "keep2");
        var original = new List<MessageExt> { a, b, c };

        List<MessageExt> dropped = DefaultMQPushConsumer.DroppedMessages(
            original, new List<MessageExt> { a, c });
        Assert.Single(dropped);
        Assert.Equal("B", dropped[0].MsgId);

        Assert.Empty(DefaultMQPushConsumer.DroppedMessages(original, original));
    }

    [Fact]
    public void FilterMessageContext_DefaultsMatchLegacy()
    {
        var ctx = new FilterMessageContext();
        Assert.Empty(ctx.MsgList);
        Assert.False(ctx.UnitMode);
        Assert.Null(ctx.Arg);
        Assert.Null(ctx.Mq);
    }
}
