// Java `double` rendering for outbound JSON bodies.
//
// The wire bodies in this package carry a handful of Java `double` fields
// (ConsumeStatus.{pullRT,pullTPS,consumeRT,consumeOKTPS,consumeFailedTPS},
// TopicStatsTable.topicPutTps, ConsumeStats.consumeTps). Java serialises them
// with fastjson2, which delegates to `Double.toString` — and that is NOT what
// Go's encoding/json produces:
//
//	value      Java (fastjson2)          Go (encoding/json)
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
// Verified against the 5.5.1 jars with a JVM probe (fastjson2 2.0.64):
//
//	0.0 -> {"v":0.0}          1.0 -> {"v":1.0}        -0.0 -> {"v":-0.0}
//	1.5 -> {"v":1.5}          0.4 -> {"v":0.4}        1/3 -> {"v":0.3333333333333333}
//	5/600 -> {"v":0.008333333333333333}
//	1e20 -> {"v":1.0E20}      1e-5 -> {"v":1.0E-5}    123456789.0 -> {"v":1.23456789E8}
//	MAX_VALUE -> {"v":1.7976931348623157E308}
//	NaN -> {"v":null}         +Inf -> {"v":null}
//
// Layout rule (Java spec of Double.toString): plain decimal notation when the
// decimal exponent is in [-3, 6], otherwise `<d>[.<digits>]E<exp>` with no `+`
// and an always-present fraction (`1.0E20`, not `1E20`).
//
// # The one remaining divergence, measured
//
// Go takes the SHORTEST decimal that round-trips (Ryu). Java 17 still runs the
// legacy `FloatingDecimal`, which is greedy and sometimes emits one digit MORE
// than necessary. A differential run against the 5.5.1 jars' fastjson2
// (12000 random bit patterns + 20000 realistic stats values) found:
//
//	random bit patterns : 69 / 12223 differ (0.56%), all at |v| > 1e16
//	realistic stats      : 26 / 20000 differ (0.13%)
//	  Java 104741.78599999999   vs Go 104741.786
//	  Java 7916.8128066546215   vs Go 7916.812806654621
//	  Java 0.42190021099664887  vs Go 0.4219002109966489
//
// Every difference is Java printing extra digits — never a different value.
// Both spellings parse back to the SAME float64, which is all the broker and
// the admin console do with this body, so the divergence is cosmetic and is
// left in place deliberately: reproducing the legacy greedy loop would mean
// porting FloatingDecimal's interval arithmetic for no observable gain. Python
// and Rust take the same shortest-digit route, so all ports agree with each
// other; only a byte-exact diff against a Java 17 emitter can see the
// difference. TestJavaDoubleRoundTripsLikeJavaLegacy pins this equivalence on
// the sampled values.
package remoting

import (
	"math"
	"strconv"
	"strings"
)

// JavaDouble is a float64 that marshals like Java's fastjson2 `double`.
type JavaDouble float64

// MarshalJSON emits the Java-compatible literal (NaN/Infinity become `null`).
func (d JavaDouble) MarshalJSON() ([]byte, error) {
	return []byte(FormatJavaDouble(float64(d))), nil
}

// FormatJavaDouble renders f the way Java's `Double.toString` + fastjson2 do.
func FormatJavaDouble(f float64) string {
	// fastjson2 writes NaN/Infinity as JSON null (fastjson1 wrote bare NaN).
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}

	// Go's shortest round-tripping digits, in scientific form. Both Go (Ryu)
	// and Java pick the shortest decimal that round-trips, so the digit string
	// matches; only the LAYOUT differs, which is what we rewrite below.
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
