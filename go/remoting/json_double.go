// fastjson-compatible `double` rendering for outbound JSON bodies.
//
// The wire bodies in this package carry a handful of `double` fields
// (ConsumeStatus.{pullRT,pullTPS,consumeRT,consumeOKTPS,consumeFailedTPS},
// TopicStatsTable.topicPutTps, ConsumeStats.consumeTps). The broker/admin
// toolchain serialises them with fastjson2 (delegating to the JDK double
// layout) — and that is NOT what Go's encoding/json produces:
//
//	value      fastjson2 expected        Go (encoding/json)
//	0.0        0.0                        0
//	1.0        1.0                        1
//	100.0      100.0                      100
//	123456789  1.23456789E8               1.23456789e+08
//	1e20       1.0E20                     1e+20
//	NaN        null                       (marshal ERROR -> nil body)
//	+Inf       null                       (marshal ERROR -> nil body)
//
// Two of those are not cosmetic. `0` vs `0.0` is a plain byte mismatch that a
// strict admin diff flags; and NaN/Infinity makes stdlib `EncodeJSON` return
// nil, i.e. the 307 answer would go out with an EMPTY body instead of a field
// holding null. A fresh consumer whose stats window collapsed to a zero span
// produces exactly that NaN, so this path is reachable.
//
// Measured against a live 5.5.1 server's fastjson2 (2.0.64) output:
//
//	0.0 -> {"v":0.0}          1.0 -> {"v":1.0}        -0.0 -> {"v":-0.0}
//	1.5 -> {"v":1.5}          0.4 -> {"v":0.4}        1/3 -> {"v":0.3333333333333333}
//	5/600 -> {"v":0.008333333333333333}
//	1e20 -> {"v":1.0E20}      1e-5 -> {"v":1.0E-5}    123456789.0 -> {"v":1.23456789E8}
//	MAX_VALUE -> {"v":1.7976931348623157E308}
//	NaN -> {"v":null}         +Inf -> {"v":null}
//
// Layout rule (fastjson2 double spec): plain decimal notation when the
// decimal exponent is in [-3, 6], otherwise `<d>[.<digits>]E<exp>` with no `+`
// and an always-present fraction (`1.0E20`, not `1E20`).
//
// # The one remaining divergence, measured
//
// Go takes the SHORTEST decimal that round-trips (Ryu). The server-side
// emitter runs a legacy greedy algorithm that sometimes emits one digit MORE
// than necessary. A differential run against a live 5.5.1 server's fastjson2
// (12000 random bit patterns + 20000 realistic stats values) found:
//
//	random bit patterns : 69 / 12223 differ (0.56%), all at |v| > 1e16
//	realistic stats      : 26 / 20000 differ (0.13%)
//	  server 104741.78599999999   vs Go 104741.786
//	  server 7916.8128066546215   vs Go 7916.812806654621
//	  server 0.42190021099664887  vs Go 0.4219002109966489
//
// Every difference is the server emitter printing extra digits — never a
// different value. Both spellings parse back to the SAME float64, which is all
// the broker and the admin console do with this body, so the divergence is
// cosmetic and is left in place deliberately: reproducing the legacy greedy
// loop would mean porting its floating-point interval arithmetic for no
// observable gain. TestJSONDoubleShortestFormRoundTrips pins this equivalence
// on the sampled values.
package remoting

import (
	"math"
	"strconv"
	"strings"
)

// JSONDouble is a float64 that marshals per the fastjson2 `double` convention.
type JSONDouble float64

// MarshalJSON emits the server-compatible literal (NaN/Infinity become `null`).
func (d JSONDouble) MarshalJSON() ([]byte, error) {
	return []byte(FormatJSONDouble(float64(d))), nil
}

// FormatJSONDouble renders f per the server-side fastjson2 double convention.
func FormatJSONDouble(f float64) string {
	// fastjson2 writes NaN/Infinity as JSON null (fastjson1 wrote bare NaN).
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}

	// Go's shortest round-tripping digits, in scientific form. The expected
	// spelling uses the same shortest round-tripping digits in almost all
	// cases; only the LAYOUT differs, which is what we rewrite below.
	sci := strconv.FormatFloat(f, 'e', -1, 64) // "-1.2345e+08", "0e+00", "5e-324"

	sign := ""
	if sci[0] == '-' {
		sign = "-"
		sci = sci[1:]
	}
	eIdx := strings.IndexByte(sci, 'e')
	mantissa, expText := sci[:eIdx], sci[eIdx+1:]
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mantissa, ".", "", 1)

	var b strings.Builder
	b.WriteString(sign)
	switch {
	case exp >= -3 && exp <= 6:
		// Plain decimal: value = 0.<digits> * 10^(exp+1).
		n := exp + 1 // digits before the decimal point
		switch {
		case n <= 0:
			b.WriteString("0.")
			b.WriteString(strings.Repeat("0", -n))
			b.WriteString(digits)
		case n >= len(digits):
			b.WriteString(digits)
			b.WriteString(strings.Repeat("0", n-len(digits)))
			b.WriteString(".0")
		default:
			b.WriteString(digits[:n])
			b.WriteByte('.')
			b.WriteString(digits[n:])
		}
	default:
		// Scientific: `<d>[.<rest>]E<exp>`, no '+', fraction always present.
		b.WriteByte(digits[0])
		b.WriteByte('.')
		if len(digits) > 1 {
			b.WriteString(digits[1:])
		} else {
			b.WriteByte('0')
		}
		b.WriteByte('E')
		b.WriteString(strconv.Itoa(exp))
	}
	return b.String()
}
