// OpenTracing hook trio unit tests: span names, tag sets, close-out rules and
// propagation write-back for the send / consume / end-transaction legs, on a
// recording fake tracer -- no cluster needed.
using System.Collections.Generic;

using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

public class OpenTracingHookTests
{
    // ---------------------------------------------------------------- fake tracer

    private sealed class FakeSpanContext
    {
        public FakeSpanContext(int id) => Id = id;

        public int Id { get; }
    }

    private sealed class FakeSpan : IOpenTracingSpan
    {
        public FakeSpan(string operation, IReadOnlyDictionary<string, string> tags, IReadOnlyList<object>? refs)
        {
            Operation = operation;
            Tags = new Dictionary<string, string>(tags);
            Refs = refs != null ? new List<object>(refs) : new List<object>();
        }

        public string Operation { get; }
        public Dictionary<string, string> Tags { get; }
        public List<object> Refs { get; }
        public bool Finished { get; private set; }
        public FakeSpanContext? SpanContext { get; private set; }
        private int _ctxSeq;

        public void SetTag(string key, string value) => Tags[key] = value;

        public object? Context() => SpanContext ??= new FakeSpanContext(++_ctxSeq);

        public void Finish() => Finished = true;
    }

    private sealed class FakeTracer : IOpenTracingTracer
    {
        public FakeTracer(object? upstream = null) => Upstream = upstream;

        public List<FakeSpan> Spans { get; } = new();
        public List<object> Injected { get; } = new();
        private object? Upstream { get; }

        public IOpenTracingSpan StartSpan(string operation, IReadOnlyDictionary<string, string> tags,
            IReadOnlyList<object>? refs)
        {
            var span = new FakeSpan(operation, tags, refs);
            Spans.Add(span);
            return span;
        }

        public object? Extract(PropertyMap carrier) => Upstream;

        public void Inject(object spanContext, PropertyMap carrier)
        {
            carrier["traceparent"] = "fake-trace-context";
            Injected.Add(spanContext);
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Message MakeSendMsg() =>
        new("TopicTest", System.Text.Encoding.UTF8.GetBytes("hello-world"))
        {
            Tags = "TagA",
            Keys = "KeyA",
        };

    private static MessageExt MakeExt(int storeSize = 42, int retry = 2)
    {
        var m = new MessageExt
        {
            Topic = "TopicTest",
            MsgId = "EXT-MSG-ID",
            Body = System.Text.Encoding.UTF8.GetBytes("xxxxxxxxxx"),
            StoreSize = storeSize,
            ReconsumeTimes = retry,
        };
        m.Properties[MessageConst.PropertyMsgRegion] = "DefaultRegion";
        m.Properties[MessageConst.PropertyTags] = "TagC";
        m.Properties[MessageConst.PropertyKeys] = "KeyC";
        return m;
    }

    private static string Tag(FakeSpan s, string key) =>
        s.Tags.TryGetValue(key, out var v) ? v : string.Empty;

    // ---------------------------------------------------------------- send leg

    [Fact]
    public void SendBeforeOpensProducerSpanWithDocumentedTags()
    {
        var tracer = new FakeTracer();
        var hook = new SendMessageOpenTracingHook(tracer);
        var ctx = new SendMessageContext
        {
            Message = MakeSendMsg(),
            BrokerAddr = "127.0.0.1:10911",
            MsgType = TraceMessageType.Normal,
        };
        hook.SendMessageBefore(ctx);

        Assert.Single(tracer.Spans);
        var span = tracer.Spans[0];
        Assert.Equal("To_TopicTest", span.Operation);
        Assert.Equal(OpenTracingTags.SpanKindProducer, Tag(span, OpenTracingTags.SpanKind));
        Assert.Equal("rocketmq", Tag(span, OpenTracingTags.PeerService));
        Assert.Equal("TopicTest", Tag(span, OpenTracingTags.MessageBusDestination));
        Assert.Equal("TagA", Tag(span, OpenTracingTags.RocketmqTags));
        Assert.Equal("KeyA", Tag(span, OpenTracingTags.RocketmqKeys));
        Assert.Equal("127.0.0.1:10911", Tag(span, OpenTracingTags.RocketmqStoreHost));
        Assert.Equal("NORMAL_MSG", Tag(span, OpenTracingTags.RocketmqMsgType));
        Assert.Equal("11", Tag(span, OpenTracingTags.RocketmqBodyLength));
        Assert.False(span.Finished);
        // the span rides in the MqTraceContext slot (Java setMqTraceContext)
        Assert.Same(span, ctx.MqTraceContext);

        // close-out: success + msg_id + region_id
        ctx.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = "MSG-ID-1",
            RegionId = "DefaultRegion",
        };
        hook.SendMessageAfter(ctx);
        Assert.Equal("true", Tag(span, OpenTracingTags.RocketmqSuccess));
        Assert.Equal("MSG-ID-1", Tag(span, OpenTracingTags.RocketmqMsgId));
        Assert.Equal("DefaultRegion", Tag(span, OpenTracingTags.RocketmqRegionId));
        Assert.True(span.Finished);
    }

    [Fact]
    public void SendAfterNoResultOrRegionIdLeavesSpanOpen()
    {
        var tracer = new FakeTracer();
        var hook = new SendMessageOpenTracingHook(tracer);
        var ctx = new SendMessageContext { Message = MakeSendMsg() };
        hook.SendMessageBefore(ctx);
        hook.SendMessageAfter(ctx);  // no send result at all
        Assert.False(tracer.Spans[0].Finished);
        ctx.SendResult = new SendResult { SendStatus = SendStatus.SendOk, MsgId = "M", RegionId = "" };
        hook.SendMessageAfter(ctx);
        Assert.False(tracer.Spans[0].Finished);
    }

    [Fact]
    public void SendAfterFailureReportsSuccessFalse()
    {
        var tracer = new FakeTracer();
        var hook = new SendMessageOpenTracingHook(tracer);
        var ctx = new SendMessageContext { Message = MakeSendMsg() };
        hook.SendMessageBefore(ctx);
        ctx.SendResult = new SendResult
        {
            SendStatus = SendStatus.FlushDiskTimeout,
            MsgId = "M",
            RegionId = "R",
        };
        hook.SendMessageAfter(ctx);
        Assert.Equal("false", Tag(tracer.Spans[0], OpenTracingTags.RocketmqSuccess));
        Assert.True(tracer.Spans[0].Finished);
    }

    [Fact]
    public void SendInjectsSpanContextIntoLiveProperties()
    {
        var tracer = new FakeTracer();
        var hook = new SendMessageOpenTracingHook(tracer);
        var msg = MakeSendMsg();
        var ctx = new SendMessageContext { Message = msg };
        hook.SendMessageBefore(ctx);
        Assert.Equal("fake-trace-context", msg.Properties["traceparent"]);
        Assert.Single(tracer.Injected);
    }

    [Fact]
    public void SendUpstreamContextBecomesRef()
    {
        var upstream = new FakeSpanContext(99);
        var tracer = new FakeTracer(upstream);
        var hook = new SendMessageOpenTracingHook(tracer);
        var ctx = new SendMessageContext { Message = MakeSendMsg() };
        hook.SendMessageBefore(ctx);
        Assert.Single(tracer.Spans[0].Refs);
        Assert.Same(upstream, tracer.Spans[0].Refs[0]);
    }

    [Fact]
    public void SendMsgTypeTagUsesJavaEnumName()
    {
        var tracer = new FakeTracer();
        var hook = new SendMessageOpenTracingHook(tracer);
        var ctx = new SendMessageContext { Message = MakeSendMsg(), MsgType = TraceMessageType.Trans };
        hook.SendMessageBefore(ctx);
        Assert.Equal("TRANS_MSG_HALF", Tag(tracer.Spans[0], OpenTracingTags.RocketmqMsgType));
        Assert.Equal("NORMAL_MSG", OpenTracingMessageTypeName.Of(TraceMessageType.Normal));
        Assert.Equal("TRANS_MSG_COMMIT", OpenTracingMessageTypeName.Of(TraceMessageType.TransCommit));
        Assert.Equal("DELAY_MSG", OpenTracingMessageTypeName.Of(TraceMessageType.Delay));
        Assert.Equal("ORDER_MSG", OpenTracingMessageTypeName.Of(TraceMessageType.Order));
    }

    // ---------------------------------------------------------------- consume leg

    [Fact]
    public void ConsumeBeforeOpensOneSpanPerMessage()
    {
        var tracer = new FakeTracer();
        var hook = new ConsumeMessageOpenTracingHook(tracer);
        var ctx = new ConsumeMessageContext("CID_test", new List<MessageExt> { MakeExt(), MakeExt() });
        hook.ConsumeMessageBefore(ctx);

        Assert.Equal(2, tracer.Spans.Count);
        foreach (var span in tracer.Spans)
        {
            Assert.Equal("From_TopicTest", span.Operation);
            Assert.Equal(OpenTracingTags.SpanKindConsumer, Tag(span, OpenTracingTags.SpanKind));
            Assert.Equal("rocketmq", Tag(span, OpenTracingTags.PeerService));
            Assert.Equal("TopicTest", Tag(span, OpenTracingTags.MessageBusDestination));
            Assert.Equal("EXT-MSG-ID", Tag(span, OpenTracingTags.RocketmqMsgId));
            Assert.Equal("TagC", Tag(span, OpenTracingTags.RocketmqTags));
            Assert.Equal("KeyC", Tag(span, OpenTracingTags.RocketmqKeys));
            Assert.Equal("42", Tag(span, OpenTracingTags.RocketmqBodyLength));
            Assert.Equal("2", Tag(span, OpenTracingTags.RocketmqRetryTimers));
            Assert.Equal("DefaultRegion", Tag(span, OpenTracingTags.RocketmqRegionId));
            Assert.False(span.Finished);
        }
        var stashed = Assert.IsType<List<IOpenTracingSpan>>(ctx.MqTraceContext);
        Assert.Equal(2, stashed.Count);
    }

    [Fact]
    public void ConsumeAfterTagsSuccessAndFinishesAll()
    {
        var tracer = new FakeTracer();
        var hook = new ConsumeMessageOpenTracingHook(tracer);
        var ctx = new ConsumeMessageContext("CID_test", new List<MessageExt> { MakeExt() });
        hook.ConsumeMessageBefore(ctx);
        ctx.Success = false;
        hook.ConsumeMessageAfter(ctx);
        Assert.Equal("false", Tag(tracer.Spans[0], OpenTracingTags.RocketmqSuccess));
        Assert.True(tracer.Spans[0].Finished);
    }

    [Fact]
    public void ConsumeEmptyMsgListIsNoop()
    {
        var tracer = new FakeTracer();
        var hook = new ConsumeMessageOpenTracingHook(tracer);
        var ctx = new ConsumeMessageContext("CID_test", new List<MessageExt>());
        hook.ConsumeMessageBefore(ctx);
        hook.ConsumeMessageAfter(ctx);
        Assert.Empty(tracer.Spans);
        Assert.Null(ctx.MqTraceContext);
    }

    // ---------------------------------------------------------------- transaction leg

    [Fact]
    public void EndTransactionOpensAndClosesImmediately()
    {
        var tracer = new FakeTracer();
        var hook = new EndTransactionOpenTracingHook(tracer);
        var ctx = new EndTransactionContext
        {
            Message = MakeSendMsg(),
            BrokerAddr = "127.0.0.1:10911",
            MsgId = "TX-MSG-ID",
            TransactionId = "TX-ID-9",
            TransactionState = LocalTransactionState.CommitMessage,
            FromTransactionCheck = true,
        };
        hook.EndTransaction(ctx);

        Assert.Single(tracer.Spans);
        var span = tracer.Spans[0];
        Assert.Equal("EndTransaction", span.Operation);
        Assert.Equal(OpenTracingTags.SpanKindProducer, Tag(span, OpenTracingTags.SpanKind));
        Assert.Equal("TopicTest", Tag(span, OpenTracingTags.MessageBusDestination));
        Assert.Equal("TagA", Tag(span, OpenTracingTags.RocketmqTags));
        Assert.Equal("KeyA", Tag(span, OpenTracingTags.RocketmqKeys));
        Assert.Equal("127.0.0.1:10911", Tag(span, OpenTracingTags.RocketmqStoreHost));
        Assert.Equal("TX-MSG-ID", Tag(span, OpenTracingTags.RocketmqMsgId));
        Assert.Equal("Trans_msg_Commit", Tag(span, OpenTracingTags.RocketmqMsgType));
        Assert.Equal("TX-ID-9", Tag(span, OpenTracingTags.RocketmqTransactionId));
        Assert.Equal("COMMIT_MESSAGE", Tag(span, OpenTracingTags.RocketmqTransactionState));
        Assert.Equal("true", Tag(span, OpenTracingTags.RocketmqIsFromTransactionCheck));
        Assert.True(span.Finished);
    }

    [Fact]
    public void EndTransactionWithoutMessageStillOpensSpan()
    {
        var tracer = new FakeTracer();
        var hook = new EndTransactionOpenTracingHook(tracer);
        hook.EndTransaction(new EndTransactionContext
        {
            MsgId = "TX",
            TransactionState = LocalTransactionState.RollbackMessage,
        });
        Assert.Equal(string.Empty, Tag(tracer.Spans[0], OpenTracingTags.MessageBusDestination));
        Assert.Equal("ROLLBACK_MESSAGE", Tag(tracer.Spans[0], OpenTracingTags.RocketmqTransactionState));
        Assert.True(tracer.Spans[0].Finished);
    }

    // ---------------------------------------------------------------- names

    [Fact]
    public void HookNamesMatchJavaImpls()
    {
        Assert.Equal("SendMessageOpenTracingHook", new SendMessageOpenTracingHook(new FakeTracer()).HookName());
        Assert.Equal("ConsumeMessageOpenTracingHook", new ConsumeMessageOpenTracingHook(new FakeTracer()).HookName());
        Assert.Equal("EndTransactionOpenTracingHook", new EndTransactionOpenTracingHook(new FakeTracer()).HookName());
    }
}
