package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// The namespace must wrap the TOPIC at the public Subscribe/Unsubscribe entry
// (Java wraps at the facade: DefaultMQPushConsumer.subscribe ->
// withNamespace(topic), DefaultLitePullConsumer likewise; 4.x pull too). Only
// the consumer group was wrapped before, so with a namespace set the
// subscription key never matched the namespaced topic the route/heartbeat/retry
// machinery speaks.

func subscriptionTopics(subs []*remoting.SubscriptionData) []string {
	out := make([]string, 0, len(subs))
	for _, s := range subs {
		out = append(out, s.Topic)
	}
	return out
}

func containsTopic(topics []string, want string) bool {
	for _, t := range topics {
		if t == want {
			return true
		}
	}
	return false
}

func TestPushSubscribeWrapsTopicWithNamespace(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_ns_sub")
	c.SetNamespace("MQ_INST_ns")

	if err := c.Subscribe("TopicA", "TagA"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	topics := subscriptionTopics(c.Subscriptions())
	if !containsTopic(topics, "MQ_INST_ns%TopicA") {
		t.Fatalf("subscription key must be the namespaced topic, got %v", topics)
	}
	if containsTopic(topics, "TopicA") {
		t.Fatalf("the raw topic must not stay as a subscription key, got %v", topics)
	}
	// Heartbeat / route fetch read the subscription key: the wrapped expression
	// must carry the namespaced topic downstream.
	if got := c.Subscriptions()[0].Topic; got != "MQ_INST_ns%TopicA" {
		t.Fatalf("subscription data topic = %q", got)
	}
}

func TestPushUnsubscribeResolvesRawNameToWrappedKey(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_ns_unsub")
	c.SetNamespace("MQ_INST_ns")
	if err := c.Subscribe("TopicA", "*"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	c.Unsubscribe("TopicA")
	if got := len(c.Subscriptions()); got != 0 {
		t.Fatalf("unsubscribe with the raw topic must drop the wrapped key, %d left", got)
	}

	// Idempotent wrap: an already-namespaced name unsubscribes the same key.
	if err := c.Subscribe("TopicB", "*"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	c.Unsubscribe("MQ_INST_ns%TopicB")
	if got := len(c.Subscriptions()); got != 0 {
		t.Fatalf("unsubscribe with the wrapped topic must drop the same key, %d left", got)
	}
}

func TestPushSubscribeSkipsWrapForSystemAndRetryShapes(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_ns_shape")
	c.SetNamespace("MQ_INST_ns")

	// System resources come back unchanged.
	if err := c.Subscribe("rmq_sys_TRACE_DATA_test", "*"); err != nil {
		t.Fatalf("subscribe system topic: %v", err)
	}
	// Retry/DLQ names keep their prefix; only the inner resource is wrapped.
	if err := c.Subscribe(common.RetryGroupTopicPrefix+"GID_ns_shape", "*"); err != nil {
		t.Fatalf("subscribe retry topic: %v", err)
	}
	topics := subscriptionTopics(c.Subscriptions())
	if !containsTopic(topics, "rmq_sys_TRACE_DATA_test") {
		t.Fatalf("system topic must not be wrapped, got %v", topics)
	}
	want := common.RetryGroupTopicPrefix + "MQ_INST_ns%GID_ns_shape"
	if !containsTopic(topics, want) {
		t.Fatalf("retry topic must keep its prefix with the inner name wrapped, want %q, got %v", want, topics)
	}
}

func TestPushSubscribeUnchangedWithoutNamespace(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_ns_free")
	if err := c.Subscribe("TopicA", "TagA"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	topics := subscriptionTopics(c.Subscriptions())
	if len(topics) != 1 || topics[0] != "TopicA" {
		t.Fatalf("without a namespace the key must stay raw, got %v", topics)
	}
}

func TestLitePullSubscribeUnsubscribeWrapTopicWithNamespace(t *testing.T) {
	c := MustNewDefaultLitePullConsumer("GID_lite_ns")
	c.SetNamespace("MQ_INST_ns")

	if err := c.Subscribe("TopicA", "*"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	exprs := c.SubscriptionExpressions()
	if _, ok := exprs["MQ_INST_ns%TopicA"]; !ok {
		t.Fatalf("lite subscription key must be the namespaced topic, got %v", exprs)
	}
	if _, ok := exprs["TopicA"]; ok {
		t.Fatalf("the raw topic must not stay as a lite subscription key, got %v", exprs)
	}
	// The route refresh + heartbeat a RUNNING lite consumer fires for the
	// subscription use the same key; verify the wrapped name is what the
	// consumer carries (Java DefaultLitePullConsumer.unsubscribe wraps too).
	c.Unsubscribe("TopicA")
	if got := c.SubscriptionExpressions(); len(got) != 0 {
		t.Fatalf("unsubscribe with the raw topic must drop the wrapped key, %v left", got)
	}
}

func TestPullSubscribeUnsubscribeWrapTopicWithNamespace(t *testing.T) {
	c := MustNewDefaultMQPullConsumer("GID_pull_ns")
	c.SetNamespace("MQ_INST_ns")

	if err := c.Subscribe("TopicA", "*"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	topics := subscriptionTopics(c.Subscriptions())
	if !containsTopic(topics, "MQ_INST_ns%TopicA") {
		t.Fatalf("pull subscription key must be the namespaced topic, got %v", topics)
	}
	c.Unsubscribe("TopicA")
	if got := len(c.Subscriptions()); got != 0 {
		t.Fatalf("unsubscribe with the raw topic must drop the wrapped key, %d left", got)
	}
	if strings.Contains(strings.Join(subscriptionTopics(c.Subscriptions()), ","), "TopicA") {
		t.Fatalf("no leftover subscription keys, got %v", c.Subscriptions())
	}
}
