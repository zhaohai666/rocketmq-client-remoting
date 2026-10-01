// Tests for the Java-alignment gap fills: producer VIP channel switch and the
// 5.x timer delay setters.
package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// The VIP channel rewrites the broker port to port-2 (common.BrokerVIPChannel,
// Java MixAll.brokerVIPChannel) only when the producer flag is on. Default off.
func TestProducerSendAddrAppliesVIPChannel(t *testing.T) {
	p, err := NewDefaultMQProducer("PID_VIP")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if p.IsSendMessageWithVIPChannel() {
		t.Fatal("sendMessageWithVIPChannel must default to false")
	}
	const addr = "127.0.0.1:10911"
	if got := p.sendAddr(addr); got != addr {
		t.Fatalf("default sendAddr = %q, want unchanged %q", got, addr)
	}
	p.SetSendMessageWithVIPChannel(true)
	if !p.IsSendMessageWithVIPChannel() {
		t.Fatal("the switch did not stick")
	}
	if got := p.sendAddr(addr); got != "127.0.0.1:10909" {
		t.Fatalf("VIP sendAddr = %q, want 127.0.0.1:10909 (port-2)", got)
	}
}

// The three 5.x timer aliases write the exact Java property keys.
func TestMessageTimerDelaySettersUseJavaPropertyNames(t *testing.T) {
	msg := common.NewMessage("T", []byte("x"))

	msg.SetDelayTimeSec(30)
	if v, ok := msg.GetProperty(common.PropertyTimerDelaySec); !ok || v != "30" {
		t.Fatalf("TIMER_DELAY_SEC = %q (ok=%v), want 30", v, ok)
	}
	msg.SetDelayTimeMs(1500)
	if v, ok := msg.GetProperty(common.PropertyTimerDelayMs); !ok || v != "1500" {
		t.Fatalf("TIMER_DELAY_MS = %q (ok=%v), want 1500", v, ok)
	}
	msg.SetDeliverTimeMs(1699999999000)
	if v, ok := msg.GetProperty(common.PropertyTimerDeliverMs); !ok || v != "1699999999000" {
		t.Fatalf("TIMER_DELIVER_MS = %q (ok=%v), want 1699999999000", v, ok)
	}
	if _, ok := msg.GetProperty(common.PropertyDelayTimeLevel); ok {
		t.Error("timer setters must not touch the 4.x DELAY level property")
	}
}
