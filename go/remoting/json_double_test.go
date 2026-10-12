package remoting

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// Guards for Go-neutral number formatting. Every expected string below was
// recorded from a live 5.5.1 server's fastjson2 2.0.64 output (see
// json_double.go for the probe transcript), so these are oracle values, not
// hand-guesses.

func TestFormatJSONDoubleMatchesWireDoubleRendering(t *testing.T) {
	// Runtime variables, not constants: Go folds untyped constant arithmetic
	// exactly, so a literal `0.1 + 0.2` is the compiler's rounding of the exact
	// 0.3 and prints "0.3" — while the runtime float64 addition gets
	// 0.30000000000000004. The point of this row is the runtime value.
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
		if got := FormatJSONDouble(c.in); got != c.want {
			t.Errorf("FormatJSONDouble(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The one that is not cosmetic: Go's stdlib encoder ERRORS on NaN/Infinity,
// which would make EncodeJSON return nil and ship an empty body. fastjson2 on
// the server writes a JSON null. A consumer whose stats window collapsed to a
// zero span produces exactly this value.
func TestFormatJSONDoubleTurnsNaNAndInfinityIntoNull(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if got := FormatJSONDouble(v); got != "null" {
			t.Errorf("FormatJSONDouble(%v) = %q, want \"null\"", v, got)
		}
	}
}

// End-to-end through the real encoder: a NaN-bearing ConsumeStatus must still
// produce a body, and that body must read `null` where the server's fastjson2
// output reads `null`.
func TestJSONDoubleSurvivesEncodeJSONWithNaN(t *testing.T) {
	cs := &ConsumeStatus{PullRT: math.NaN(), PullTPS: 0, ConsumeRT: 0, ConsumeOKTPS: 0, ConsumeFailedTPS: 0}
	body := EncodeJSON(cs.ToJSONValue())
	if body == nil {
		t.Fatal("EncodeJSON returned nil for a NaN-bearing ConsumeStatus (the server-side emitter writes a null field)")
	}
	text := string(body)
	if !strings.Contains(text, `"pullRT":null`) {
		t.Errorf("body = %s, want pullRT rendered as null", text)
	}
	// And the sibling doubles must keep the fastjson2 `.0` spelling.
	if !strings.Contains(text, `"pullTPS":0.0`) {
		t.Errorf("body = %s, want pullTPS rendered as 0.0 (not 0)", text)
	}
}

// fastjson2 writes `0.0`, Go's stdlib writes `0` — an easy accidental
// regression if someone unwraps JSONDouble back to a bare float64.
func TestJSONDoubleKeepsTrailingPointOnIntegralValues(t *testing.T) {
	body := string(EncodeJSON((&ConsumeStatus{}).ToJSONValue()))
	for _, field := range []string{"pullRT", "pullTPS", "consumeRT", "consumeOKTPS", "consumeFailedTPS"} {
		if !strings.Contains(body, `"`+field+`":0.0`) {
			t.Errorf("body = %s, want %q rendered as 0.0", body, field)
		}
	}
	// consumeFailedMsgs is a long field, not a double: plain `0`, no decimal point.
	if !strings.Contains(body, `"consumeFailedMsgs":0`) {
		t.Errorf("body = %s, want consumeFailedMsgs rendered as 0", body)
	}
	if strings.Contains(body, `"consumeFailedMsgs":0.0`) {
		t.Errorf("body = %s, consumeFailedMsgs is a long, not a double", body)
	}
}

// TopicStatsTable and ConsumeStats carry the other two doubles rendered by
// fastjson2 in this package; both were recorded as diverging from live 5.5.1
// server output before JSONDouble existed.
func TestOtherAdminBodiesAlsoUseJSONDouble(t *testing.T) {
	if body := string(NewTopicStatsTable().Encode()); !strings.Contains(body, `"topicPutTps":0.0`) {
		t.Errorf("TopicStatsTable body = %s, want topicPutTps 0.0", body)
	}
	if body := string(NewConsumeStats().Encode()); !strings.Contains(body, `"consumeTps":0.0`) {
		t.Errorf("ConsumeStats body = %s, want consumeTps 0.0", body)
	}
}

// The residual divergence is that the server's fastjson2 emitter runs a legacy
// greedy FloatingDecimal and prints ONE EXTRA digit (measured 26/20000
// realistic stats values;
// see json_double.go for the full differential). Each pair below is a real
// sample from that run, in both spellings.
//
// The assertion of interest is the second one: Go's shorter spelling parses
// back to the SAME float64 as the server's longer one. That is what makes the
// divergence cosmetic — the broker and the admin console parse these numbers,
// they never diff the bytes. If a future change breaks the round-trip, this
// fails loudly instead of quietly shipping a wrong rate.
func TestJSONDoubleShortestFormRoundTrips(t *testing.T) {
	pairs := []struct {
		longestForm  string
		shortestForm string
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
		longestVal, err := strconv.ParseFloat(p.longestForm, 64)
		if err != nil {
			t.Fatalf("bad fixture %q: %v", p.longestForm, err)
		}
		if got := FormatJSONDouble(longestVal); got != p.shortestForm {
			t.Errorf("FormatJSONDouble(%s) = %q, want the shortest form %q", p.longestForm, got, p.shortestForm)
		}
		shortestVal, err := strconv.ParseFloat(p.shortestForm, 64)
		if err != nil {
			t.Fatalf("bad fixture %q: %v", p.shortestForm, err)
		}
		if shortestVal != longestVal {
			t.Errorf("shortest %q -> %v but longest %q -> %v: the divergence is NOT cosmetic",
				p.shortestForm, shortestVal, p.longestForm, longestVal)
		}
	}
}
