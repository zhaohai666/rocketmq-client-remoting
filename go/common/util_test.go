package common

import (
	"strings"
	"testing"
	"time"
)

func TestJavaHashMatchesReferenceVectors(t *testing.T) {
	cases := []struct {
		in   string
		want int32
	}{
		{"TagA", 2598919},
		{"TagB", 2598920},
		{"P", 80},
		{"PA", 2545},
		{"*", 42},
		{"", 0},
		{"\u4e2d", 20013},
	}
	for _, c := range cases {
		if got := JavaStringHash(c.in); got != c.want {
			t.Fatalf("JavaStringHash(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestCrc32MatchesZlib(t *testing.T) {
	if Crc32(nil) != 0 {
		t.Fatal("crc32(\"\") != 0")
	}
	if Crc32([]byte("1234567890")) != 639479525 {
		t.Fatal("crc32(1234567890) mismatch")
	}
	if Crc32([]byte("the quick brown fox")) != 2445345482 {
		t.Fatal("crc32(quick fox) mismatch")
	}
	if Crc32([]byte("a")) != 0xE8B7BE43 {
		t.Fatal("crc32(a) mismatch")
	}
	if Crc32([]byte("abc")) != 0x352441C2 {
		t.Fatal("crc32(abc) mismatch")
	}
}

func TestHexRoundTrip(t *testing.T) {
	b := []byte{0x00, 0x0F, 0xFF, 0xA5}
	hex := Bytes2String(b)
	if hex != "000FFFA5" {
		t.Fatalf("got %s", hex)
	}
	if back := String2Bytes(hex); back == nil || string(back) != string(b) {
		t.Fatalf("String2Bytes: %v", back)
	}
	if String2Bytes("") != nil {
		t.Fatal("empty hex should be nil")
	}
	if String2Bytes("XYZ") != nil {
		t.Fatal("bad hex should be nil")
	}
}

func TestOffsetFilenameIsZeroPadded20(t *testing.T) {
	if Offset2Filename(0) != "00000000000000000000" || Offset2Filename(12345) != "00000000000000012345" {
		t.Fatal("offset filename padding mismatch")
	}
}

func TestBlankHelpers(t *testing.T) {
	if !IsBlank("") || !IsBlank("  \t") || IsBlank("x") || !IsNotBlank("x") {
		t.Fatal("blank helpers mismatch")
	}
}

func TestAddrParsing(t *testing.T) {
	if h, p := ParseAddr("127.0.0.1:9876"); h != "127.0.0.1" || p != "9876" {
		t.Fatalf("got %s %s", h, p)
	}
	if h, p := ParseAddr("[::1]:9876"); h != "::1" || p != "9876" {
		t.Fatalf("got %s %s", h, p)
	}
	if !IsIPv4("127.0.0.1") || IsIPv4("::1") || !IsIPv6("::1") {
		t.Fatal("ip classification mismatch")
	}
}

func TestHumanTimeUsesDashForZero(t *testing.T) {
	if TimeToHumanString(0) != "-" || TimeToHumanString(-1) != "-" {
		t.Fatal("zero/negative must render as -")
	}
	if got := TimeToHumanString(1700000000000); len(got) != 19 {
		t.Fatalf("got %q", got)
	}
	// Local timezone round trip: parse back and compare within a minute.
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", TimeToHumanString(1700000000000), time.Local)
	if err != nil {
		t.Fatal(err)
	}
	if diff := parsed.UnixMilli() - 1700000000000; diff < -60000 || diff > 60000 {
		t.Fatalf("round trip drift %dms", diff)
	}
}

func TestUniqIDShapeAndUniqueness(t *testing.T) {
	first := CreateUniqID()
	second := CreateUniqID()
	if first == second {
		t.Fatal("consecutive ids must differ")
	}
	if IsIPv4(LocalIP()) && len(first) != 32 {
		t.Fatalf("IPv4 ids are 32 chars, got %q", first)
	}
	for _, c := range first {
		if !isUpperHex(c) {
			t.Fatalf("only uppercase hex allowed, got %q", first)
		}
	}
	n := func(s string) int64 {
		var v int64
		for _, c := range s[len(s)-4:] {
			v = v*16 + int64(hexVal(byte(c)))
		}
		return v
	}
	if n(second) != n(first)+1 {
		t.Fatalf("counter must increment: %s -> %s", first, second)
	}
}

func isUpperHex(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	}
	return 0
}

func TestDayMillisStaysInOneDay(t *testing.T) {
	if ms := CurrentDayMillis(); ms >= 86400000 {
		t.Fatalf("day millis overflow: %d", ms)
	}
}

func TestMonotonicAndNanoClocks(t *testing.T) {
	a := MonotonicMillis()
	time.Sleep(2 * time.Millisecond)
	b := MonotonicMillis()
	if b < a {
		t.Fatalf("monotonic clock went backwards: %f -> %f", a, b)
	}
	if NanoTime() <= 0 {
		t.Fatal("nano time must be positive")
	}
	if !strings.Contains(TimeToHumanString(CurrentTimeMillis()), "-") {
		t.Fatal("unexpected human time shape")
	}
}
