// Java MessageClientExt#getMsgId() parity on consumed messages.
//
// Client-side decoding yields a MessageClientExt whose getMsgId() returns the
// UNIQ_KEY property, falling back to the broker offset id only when that
// property is absent. This port keeps the offset id in MsgID, so the three
// consumer-side call sites (SubBefore bean, CONSUMER_SEND_MSG_BACK header,
// ORIGIN_MESSAGE_ID retry fallback) must all go through common.ClientMsgID.
package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	parityOffsetID = "0A0B0C0D0000000000000000000EA7"
	parityUniqID   = "AC1E0A0B2C3D4E5F60718293A4B5C6D7"
)

func parityConsumed(withUniq bool) *common.MessageExt {
	msg := common.NewMessageExt()
	msg.Topic = "TopicTest"
	msg.MsgID = parityOffsetID
	msg.Body = []byte("body")
	msg.StoreTimestamp = 1700000000000
	msg.StoreSize = 42
	msg.ReconsumeTimes = 1
	if withUniq {
		msg.PutProperty(common.PropertyUniqKey, parityUniqID)
	}
	return msg
}

func TestClientMsgIDHelperPrefersUniqKey(t *testing.T) {
	if got := common.ClientMsgID(parityConsumed(true)); got != parityUniqID {
		t.Errorf("ClientMsgID(with UNIQ_KEY) = %q, want %q", got, parityUniqID)
	}
	if got := common.ClientMsgID(parityConsumed(false)); got != parityOffsetID {
		t.Errorf("ClientMsgID(no UNIQ_KEY) = %q, want the offset id %q", got, parityOffsetID)
	}
	empty := parityConsumed(true)
	empty.PutProperty(common.PropertyUniqKey, "")
	if got := common.ClientMsgID(empty); got != parityOffsetID {
		t.Errorf("ClientMsgID(empty UNIQ_KEY) = %q, want the offset id %q", got, parityOffsetID)
	}
	if got := common.ClientMsgID(nil); got != "" {
		t.Errorf("ClientMsgID(nil) = %q, want empty", got)
	}
}

func TestClientMsgIDSubBeforeBean(t *testing.T) {
	for _, tc := range []struct {
		name     string
		withUniq bool
		want     string
	}{
		{"uniq key", true, parityUniqID},
		{"offset fallback", false, parityOffsetID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDispatcher("CID_test", TraceDispatcherConsume, 1, "")
			hook := NewConsumeMessageTraceHook(d)
			ctx := &ConsumeMessageContext{
				ConsumerGroup: "CID_test",
				MsgList:       []*common.MessageExt{parityConsumed(tc.withUniq)},
			}
			hook.ConsumeMessageBefore(ctx)
			select {
			case got := <-d.traceQueue:
				if got.TraceType != TraceSubBefore {
					t.Fatalf("traceType = %v, want SubBefore", got.TraceType)
				}
				if got.TraceBeans[0].MsgID != tc.want {
					t.Errorf("SubBefore bean msgId = %q, want %q", got.TraceBeans[0].MsgID, tc.want)
				}
			default:
				t.Fatal("before hook appended nothing")
			}
		})
	}
}

func TestClientMsgIDBuildRetryMessageOriginID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		withUniq bool
		want     string
	}{
		{"uniq key", true, parityUniqID},
		{"offset fallback", false, parityOffsetID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := buildRetryMessage(parityConsumed(tc.withUniq), "GID_test", 16)
			v, _ := got.GetProperty(common.PropertyOriginMessageID)
			if v != tc.want {
				t.Errorf("retry ORIGIN_MESSAGE_ID = %q, want %q", v, tc.want)
			}
		})
	}

	// An explicit ORIGIN_MESSAGE_ID on the source still wins over both.
	msg := parityConsumed(true)
	msg.PutProperty(common.PropertyOriginMessageID, "EXPLICIT")
	got := buildRetryMessage(msg, "GID_test", 16)
	if v, _ := got.GetProperty(common.PropertyOriginMessageID); v != "EXPLICIT" {
		t.Errorf("retry ORIGIN_MESSAGE_ID = %q, want the explicit value", v)
	}
}

func TestClientMsgIDSendBackHeaderUsesUniqKey(t *testing.T) {
	topic := uniqueTopic("GoClientMsgIDSendBack", t)
	const group = "GID_go_client_msg_id_sendback"

	f := newClusterFixture(t, map[string]int{topic: 1})
	msgs := f.broker.add(topic, 0, "poison")
	uniq, _ := msgs[0].GetProperty(common.PropertyUniqKey)
	if uniq == "" {
		t.Fatal("fixture message must carry a UNIQ_KEY")
	}
	listener := newRecordingListener()
	listener.setStatus(ReconsumeLater)
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "send-back", func() bool { return len(f.broker.sendBackSnapshot()) > 0 })
	back := f.broker.sendBackSnapshot()[0]
	if got := deref(back.OriginMsgID); got != uniq {
		t.Errorf("send-back originMsgId = %q, want the UNIQ_KEY %q", got, uniq)
	}
}
