package remoting

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// Guards for Go-neutral number formatting. Every expected string below was
// produced by running the 5.5.1 jars' fastjson2 2.0.64 (see java_float.go for
// the probe transcript), so these are oracle values, not hand-guesses.

func TestFormatJavaDoubleMatchesJavaDoubleToString(t *testing.T) {
	// Runtime variables, not constants: Go folds untyped constant arithmetic
	// exactly, so a literal `0.1 + 0.2` is the compiler's rounding of the exact
	// 0.3 and prints "0.3" — while Java (and Go) add two float64s at runtime and
	// get 0.30000000000000004. The point of this row is the runtime value.
	a, b := 0.1, 0.2

	cases := []struct {
		in   float64
		want string
	}{
		{0, "0.0"},
		{math.Copysign(0, -1), "-0.0"},
		{1, "1.0"},
		{100, "100.0"},
		{1.5, "1.5"},
		{0.4, "0.4"},
		{-1.5, "-1.5"},

		// Plain/scientific switch is at decimal exponent [-3, 6].
		{0.001, "0.001"},
		{0.008333333333333333, "0.008333333333333333"},
		{1.0 / 3.0, "0.3333333333333333"},
		{a + b, "0.30000000000000004"},
		{1234567, "1234567.0"},
		{12345678, "1.2345678E7"},
		{123456789, "1.23456789E8"},
		{1e20, "1.0E20"},
		{1e-5, "1.0E-5"},
		{math.MaxFloat64, "1.7976931348623157E308"},
	}
	for _, c := range cases {
		if got := FormatJavaDouble(c.in); got != c.want {
			t.Errorf("FormatJavaDouble(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The one that is not cosmetic: Go's stdlib encoder ERRORS on NaN/Infinity,
// which would make EncodeJSON return nil and ship an empty body. Java writes a
// JSON null. A consumer whose stats window collapsed to a zero span produces
// exactly this value.
func TestFormatJavaDoubleTurnsNaNAndInfinityIntoJSONNull(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if got := FormatJavaDouble(v); got != "null" {
			t.Errorf("FormatJavaDouble(%v) = %q, want \"null\"", v, got)
		}
	}
}

// End-to-end through the real encoder: a NaN-bearing ConsumeStatus must still
// produce a body, and that body must read `null` where Java reads `null`.
func TestJavaDoubleSurvivesEncodeJSONWithNaN(t *testing.T) {
	cs := &ConsumeStatus{PullRT: math.NaN(), PullTPS: 0, ConsumeRT: 0, ConsumeOKTPS: 0, ConsumeFailedTPS: 0}
	body := EncodeJSON(cs.ToJSONValue())
	if body == nil {
		t.Fatal("EncodeJSON returned nil for a NaN-bearing ConsumeStatus (Java emits a null field)")
	}
	text := string(body)
	if !strings.Contains(text, `"pullRT":null`) {
		t.Errorf("body = %s, want pullRT rendered as null", text)
	}
	// And the sibling doubles must keep their Java `.0` spelling.
	if !strings.Contains(text, `"pullTPS":0.0`) {
		t.Errorf("body = %s, want pullTPS rendered as 0.0 (not 0)", text)
	}
}

// Java's fastjson2 writes `0.0`, Go's stdlib writes `0` — an easy accidental
// regression if someone unwraps JavaDouble back to a bare float64.
func TestJavaDoubleKeepsTheTrailingPointOnIntegralValues(t *testing.T) {
	body := string(EncodeJSON((&ConsumeStatus{}).ToJSONValue()))
	for _, field := range []string{"pullRT", "pullTPS", "consumeRT", "consumeOKTPS", "consumeFailedTPS"} {
		if !strings.Contains(body, `"`+field+`":0.0`) {
			t.Errorf("body = %s, want %q rendered as 0.0", body, field)
		}
	}
	// consumeFailedMsgs is a Java long: plain `0`, no decimal point.
	if !strings.Contains(body, `"consumeFailedMsgs":0`) {
		t.Errorf("body = %s, want consumeFailedMsgs rendered as 0", body)
	}
	if strings.Contains(body, `"consumeFailedMsgs":0.0`) {
		t.Errorf("body = %s, consumeFailedMsgs is a long, not a double", body)
	}
}

// TopicStatsTable and ConsumeStats carry the other two Java doubles in this
// package; both were recorded as diverging before JavaDouble existed.
func TestOtherAdminBodiesAlsoUseJavaDouble(t *testing.T) {
	if body := string(NewTopicStatsTable().Encode()); !strings.Contains(body, `"topicPutTps":0.0`) {
		t.Errorf("TopicStatsTable body = %s, want topicPutTps 0.0", body)
	}
	if body := string(NewConsumeStats().Encode()); !strings.Contains(body, `"consumeTps":0.0`) {
		t.Errorf("ConsumeStats body = %s, want consumeTps 0.0", body)
	}
}

// The residual divergence with Java 17 is that its legacy FloatingDecimal is
// greedy and prints ONE EXTRA digit (measured 26/20000 realistic stats values;
// see java_float.go for the full differential). Each pair below is a real
// sample from that run, in both spellings.
//
// The assertion of interest is the second one: Go's shorter spelling parses
// back to the SAME float64 as Java's longer one. That is what makes the
// divergence cosmetic — the broker and the admin console parse these numbers,
// they never diff the bytes. If a future change breaks the round-trip, this
// fails loudly instead of quietly shipping a wrong rate.
func TestJavaDoubleRoundTripsLikeJavaLegacy(t *testing.T) {
	pairs := []struct {
		java   string
		goWant string
	}{
		{"104741.78599999999", "104741.786"},
		{"78262.33900000001", "78262.339"},
		{"7916.8128066546215", "7916.812806654621"},
		{"0.42190021099664887", "0.4219002109966489"},
		{"2539.3993893435472", "2539.399389343547"},
		{"60946.216303994806", "60946.21630399481"},
		{"10169.578957152151", "10169.57895715215"},
		{"0.8374433194608361", "0.837443319460836"},
		// Far end of the range, where the extra digits show up most often.
		{"-4.325857695266085888E18", "-4.325857695266086E18"},
		{"8.9195188668911552E17", "8.919518866891155E17"},
		{"1.6699629581838839E86", "1.669962958183884E86"},
	}
	for _, p := range pairs {
		javaVal, err := strconv.ParseFloat(p.java, 64)
		if err != nil {
			t.Fatalf("bad fixture %q: %v", p.java, err)
		}
		if got := FormatJavaDouble(javaVal); got != p.goWant {
			t.Errorf("FormatJavaDouble(%s) = %q, want the shortest form %q", p.java, got, p.goWant)
		}
		goVal, err := strconv.ParseFloat(p.goWant, 64)
		if err != nil {
			t.Fatalf("bad fixture %q: %v", p.goWant, err)
		}
		if goVal != javaVal {
			t.Errorf("Go %q -> %v but Java %q -> %v: the divergence is NOT cosmetic",
				p.goWant, goVal, p.java, javaVal)
		}
	}
}
