package remoting

import (
	"strings"
	"testing"
)

// A hand-built SubscriptionGroupConfig (a struct literal, i.e. WITHOUT the
// constructor) leaves GroupRetryPolicy nil. Encoding it used to dereference that
// nil pointer and panic the whole request — found by live_admin against a real
// broker, where no unit test had ever built the config this way.
func TestSubscriptionGroupConfigNilRetryPolicyDoesNotPanic(t *testing.T) {
	cfg := &SubscriptionGroupConfig{GroupName: "G", RetryMaxTimes: 5}

	body := cfg.Encode() // must not panic
	text := string(body)
	if strings.Contains(text, "groupRetryPolicy") {
		t.Fatalf("nil retry policy must be dropped like fastjson2 drops nulls: %s", text)
	}
	// The rest of the config still has to be on the wire.
	for _, want := range []string{`"groupName":"G"`, `"retryMaxTimes":5`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
}

// The non-nil case keeps the key, with the Java field spelling.
func TestSubscriptionGroupConfigRetryPolicyIsSerialisedWhenSet(t *testing.T) {
	cfg := NewSubscriptionGroupConfig("G")

	text := string(cfg.Encode())
	if !strings.Contains(text, `"groupRetryPolicy":{"type":"CUSTOMIZED"}`) {
		t.Fatalf("groupRetryPolicy missing or misspelled: %s", text)
	}
}

// Round trip: a nil policy encodes to an absent key, and decoding an absent key
// restores the Java default (the field is initialised in Java's constructor, so
// it is never null on the way back in).
func TestSubscriptionGroupConfigNilRetryPolicyRoundTrip(t *testing.T) {
	origin := &SubscriptionGroupConfig{GroupName: "G", RetryMaxTimes: 5, ConsumeEnable: true}

	back, err := DecodeSubscriptionGroupConfig(origin.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if back.GroupName != "G" || back.RetryMaxTimes != 5 || !back.ConsumeEnable {
		t.Fatalf("back = %s", back)
	}
	if back.GroupRetryPolicy == nil {
		t.Fatal("decoded config must carry the Java default retry policy")
	}
	if back.GroupRetryPolicy.Type != GroupRetryPolicyCustomized {
		t.Fatalf("retry policy type = %q", back.GroupRetryPolicy.Type)
	}
}
