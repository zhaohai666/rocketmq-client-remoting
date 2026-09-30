package common

import (
	"encoding/base64"
	"testing"
)

// The vector comes from Java RecallMessageHandleTest / the Rust port's test:
// build_handle("TopicA", "broker-a", "1700000000000",
// "0123456789ABCDEF0123456789abcdef") must reproduce Java's PADDED url-base64
// byte for byte.
func TestBuildRecallHandleJavaVector(t *testing.T) {
	const want = "djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgMDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY="
	got := BuildRecallHandle("TopicA", "broker-a", "1700000000000", "0123456789ABCDEF0123456789abcdef")
	if got != want {
		t.Fatalf("BuildRecallHandle = %q, want %q", got, want)
	}
}

func TestDecodeRecallHandleRoundTripAndPadding(t *testing.T) {
	padded := BuildRecallHandle("TopicA", "broker-a", "1700000000000", "UNIQ-KEY-1")
	if !hasPaddedSuffix(padded) {
		t.Fatalf("BuildRecallHandle must emit padding (Java urlEncoder), got %q", padded)
	}
	unpadded := base64.RawURLEncoding.EncodeToString([]byte("v1 TopicA broker-a 1700000000000 UNIQ-KEY-1"))

	for _, tc := range []struct {
		name   string
		handle string
	}{
		{"padded", padded},
		{"unpadded", unpadded},
		{"double padded", padded + "="},
	} {
		h, err := DecodeRecallHandle(tc.handle)
		if err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if h.Topic != "TopicA" || h.BrokerName != "broker-a" ||
			h.TimestampStr != "1700000000000" || h.MessageID != "UNIQ-KEY-1" {
			t.Fatalf("%s: decoded %+v", tc.name, h)
		}
	}
}

func hasPaddedSuffix(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '='
}

func TestDecodeRecallHandleInvalid(t *testing.T) {
	v2 := base64.URLEncoding.EncodeToString([]byte("v2 TopicA broker-a 1700000000000 MID"))
	short := base64.URLEncoding.EncodeToString([]byte("v1 TopicA broker-a"))

	for _, tc := range []struct {
		name   string
		handle string
	}{
		{"empty", ""},
		{"not base64", "not base64 !!"},
		{"bad alphabet", "!!!!"},
		{"wrong version", v2},
		{"too few segments", short},
	} {
		_, err := DecodeRecallHandle(tc.handle)
		if err == nil {
			t.Fatalf("%s: expected an error", tc.name)
		}
		if ce, ok := err.(*Error); !ok || ce.Message != RecallHandleInvalid {
			t.Fatalf("%s: error = %q, want %q", tc.name, err.Error(), RecallHandleInvalid)
		}
	}
}

// Java's decoder splits on whitespace and only reads the first five fields, so
// trailing content is ignored and items[4] stays the messageID.
func TestDecodeRecallHandleExtraSegmentsIgnored(t *testing.T) {
	handle := base64.URLEncoding.EncodeToString([]byte("v1 TopicA b1 1700000000000 MID extra more"))
	h, err := DecodeRecallHandle(handle)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if h.MessageID != "MID" || h.Topic != "TopicA" || h.BrokerName != "b1" {
		t.Fatalf("decoded %+v", h)
	}
}
