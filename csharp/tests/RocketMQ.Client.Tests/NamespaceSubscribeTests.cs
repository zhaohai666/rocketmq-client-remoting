// Offline unit tests for the namespace subscribe surface (the Java facade
// semantics). Java wraps the TOPIC at the public Subscribe entry
// (DefaultMQPushConsumer.subscribe:838 -> withNamespace(topic)); Unsubscribe
// resolves a raw caller name to the wrapped subscription key (see the note on
// the Java 5.x raw-passthrough quirk on Consumer.Unsubscribe). No cluster
// needed: Subscribe without Start only writes the subscription table (the
// table the heartbeat and the rebalance read), NotifySubscriptionChanged is a
// no-op before Start.
using System;
using System.Linq;

using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class NamespaceSubscribeTests
{
    [Fact]
    public void SubscribeWrapsTopicWithNamespace()
    {
        var c = new DefaultMQPushConsumer("CID_ns_sub_cpp_parity");
        c.Namespace = "MQ_INST_ns";

        c.Subscribe("TopicA", "TagA");
        var topics = c.SubscribedTopics();
        var expected = new[] { "MQ_INST_ns%TopicA" };
        Assert.Single(topics);
        Assert.Equal(expected, topics);
    }

    [Fact]
    public void UnsubscribeResolvesRawNameToWrappedKey()
    {
        var c = new DefaultMQPushConsumer("CID_ns_unsub_cpp_parity");
        c.Namespace = "MQ_INST_ns";

        c.Subscribe("TopicA", "*");
        c.Unsubscribe("TopicA");
        Assert.Empty(c.SubscribedTopics());

        // Idempotent wrap: an already-namespaced name unsubscribes the same key.
        c.Subscribe("TopicB", "*");
        c.Unsubscribe("MQ_INST_ns%TopicB");
        Assert.Empty(c.SubscribedTopics());
    }

    [Fact]
    public void SubscribeSkipsWrapForSystemAndRetryShapes()
    {
        var c = new DefaultMQPushConsumer("CID_ns_shape_cpp_parity");
        c.Namespace = "MQ_INST_ns";

        // System resources come back unchanged.
        c.Subscribe("rmq_sys_TRACE_DATA_test", "*");
        // Retry/DLQ names keep their prefix; only the inner resource is wrapped.
        c.Subscribe(MixAll.RetryGroupTopicPrefix + "CID_ns_shape_cpp_parity", "*");
        var topics = c.SubscribedTopics();
        Assert.Contains("rmq_sys_TRACE_DATA_test", topics);
        Assert.Contains(MixAll.RetryGroupTopicPrefix + "MQ_INST_ns%CID_ns_shape_cpp_parity", topics);
    }

    [Fact]
    public void SubscribeStaysRawWithoutNamespace()
    {
        var c = new DefaultMQPushConsumer("CID_ns_free_cpp_parity");
        c.Subscribe("TopicA", "TagA");
        var topics = c.SubscribedTopics();
        Assert.Single(topics);
        Assert.Equal("TopicA", topics[0]);
    }
}
