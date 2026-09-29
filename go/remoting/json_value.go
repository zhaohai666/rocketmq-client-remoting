// JSONValue: a fastjson2-compatible JSON decoder for broker headers.
//
// fastjson2 (the broker's JSON library) emits non-standard JSON: map keys may
// be bare numbers, map keys may be objects (offsetTable keyed by MessageQueue),
// NaN/Infinity are unquoted, and trailing commas appear in some outputs. The
// stdlib encoder rejects all of these, so the protocol layer carries its own
// tolerant parser.
//
// Decoded value shapes: nil, bool, JSONNumber (raw literal text), string,
// []any, map[string]any.
package remoting

import (
	"strconv"
	"unicode/utf8"
)

// JSONNumber keeps the raw number literal so 64-bit values round-trip exactly
// (encoding/json would coerce u64 > maxInt64 to float64).
type JSONNumber string

func (n JSONNumber) Int64() (int64, error)     { return strconv.ParseInt(string(n), 10, 64) }
func (n JSONNumber) Uint64() (uint64, error)   { return strconv.ParseUint(string(n), 10, 64) }
func (n JSONNumber) Float64() (float64, error) { return strconv.ParseFloat(string(n), 64) }

// String returns the raw literal text.
func (n JSONNumber) String() string { return string(n) }

// MarshalJSON emits the raw literal (never quoted); the parser only produces
// validated numeric literals.
func (n JSONNumber) MarshalJSON() ([]byte, error) { return []byte(n), nil }

const jsonWS = " \t\r\n"

type jsonParser struct {
	text string
	pos  int
}

// ParseJSON decodes tolerant JSON; the whole input must be consumed.
func ParseJSON(text string) (any, error) {
	p := &jsonParser{text: text}
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if !p.eof() {
		return nil, decodeErrf("json trailing data at offset %d", p.pos)
	}
	return v, nil
}

// ParseJSONBytes parses tolerant JSON from bytes.
func ParseJSONBytes(data []byte) (any, error) {
	return ParseJSON(string(data))
}

// DecodeMapKey re-parses an inline object key such as
// `{"brokerName":"b","queueId":3}` back into a value (fastjson2 writes
// MessageQueue-keyed maps this way).
func DecodeMapKey(key string) (any, bool) {
	t := trimJSONSpace(key)
	if len(t) == 0 || t[0] != '{' {
		return nil, false
	}
	v, err := ParseJSON(t)
	if err != nil {
		return nil, false
	}
	return v, true
}

func trimJSONSpace(s string) string {
	start := 0
	for start < len(s) && isJSONWS(s[start]) {
		start++
	}
	end := len(s)
	for end > start && isJSONWS(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isJSONWS(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

func (p *jsonParser) eof() bool { return p.pos >= len(p.text) }

func (p *jsonParser) peek() (byte, bool) {
	if p.eof() {
		return 0, false
	}
	return p.text[p.pos], true
}

func (p *jsonParser) skipWS() {
	for !p.eof() && isJSONWS(p.text[p.pos]) {
		p.pos++
	}
}

func (p *jsonParser) value() (any, error) {
	p.skipWS()
	c, ok := p.peek()
	if !ok {
		return nil, decodeErrf("unexpected end of input at offset %d", p.pos)
	}
	switch c {
	case '{':
		return p.object()
	case '[':
		return p.array()
	case '"':
		return p.string()
	default:
		return p.literal()
	}
}

func (p *jsonParser) object() (map[string]any, error) {
	p.pos++ // consume '{'
	out := make(map[string]any)
	p.skipWS()
	if c, ok := p.peek(); ok && c == '}' {
		p.pos++
		return out, nil
	}
	for {
		key, err := p.key()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		c, ok := p.peek()
		if !ok || c != ':' {
			return nil, decodeErrf("expected ':' at offset %d", p.pos)
		}
		p.pos++
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		out[key] = val
		p.skipWS()
		c, ok = p.peek()
		if !ok {
			return nil, decodeErrf("expected ',' or '}' at offset %d", p.pos)
		}
		switch c {
		case ',':
			p.pos++
			p.skipWS()
			// Trailing comma: allow `...,}`.
			if c2, ok := p.peek(); ok && c2 == '}' {
				p.pos++
				return out, nil
			}
		case '}':
			p.pos++
			return out, nil
		default:
			return nil, decodeErrf("expected ',' or '}' at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) array() ([]any, error) {
	p.pos++ // consume '['
	out := []any{}
	p.skipWS()
	if c, ok := p.peek(); ok && c == ']' {
		p.pos++
		return out, nil
	}
	for {
		val, err := p.value()
		if err != nil {
			return nil, err
		}
		out = append(out, val)
		p.skipWS()
		c, ok := p.peek()
		if !ok {
			return nil, decodeErrf("expected ',' or ']' at offset %d", p.pos)
		}
		switch c {
		case ',':
			p.pos++
			p.skipWS()
			// Trailing comma: allow `...,]`.
			if c2, ok := p.peek(); ok && c2 == ']' {
				p.pos++
				return out, nil
			}
		case ']':
			p.pos++
			return out, nil
		default:
			return nil, decodeErrf("expected ',' or ']' at offset %d", p.pos)
		}
	}
}

func (p *jsonParser) key() (string, error) {
	p.skipWS()
	c, ok := p.peek()
	if !ok {
		return "", decodeErrf("unexpected end of input at offset %d", p.pos)
	}
	switch c {
	case '"':
		return p.string()
	case '{', '[':
		// Non-string keys keep their raw text; the caller re-parses with
		// DecodeMapKey when the field is known to be object-keyed.
		start := p.pos
		if _, err := p.value(); err != nil {
			return "", err
		}
		return p.text[start:p.pos], nil
	default:
		start := p.pos
		for {
			c, ok := p.peek()
			if !ok {
				break
			}
			if c == ':' || isJSONWS(c) {
				break
			}
			p.pos++
		}
		return p.text[start:p.pos], nil
	}
}

func (p *jsonParser) string() (string, error) {
	p.pos++ // consume '"'
	var out []byte
	for {
		if p.pos >= len(p.text) {
			return "", decodeErrf("unterminated string")
		}
		c := p.text[p.pos]
		switch c {
		case '"':
			p.pos++
			return string(out), nil
		case '\\':
			p.pos++
			if p.pos >= len(p.text) {
				return "", decodeErrf("unterminated escape")
			}
			e := p.text[p.pos]
			switch e {
			case 'u':
				if p.pos+4 >= len(p.text) {
					return "", decodeErrf("bad \\u escape")
				}
				digits := p.text[p.pos+1 : p.pos+5]
				code, err := strconv.ParseUint(digits, 16, 32)
				if err != nil {
					return "", decodeErrf("bad \\u escape")
				}
				scalar := rune(code)
				// Surrogate pair: fastjson2 emits standard \uXXXX\uXXXX.
				if code >= 0xD800 && code < 0xDC00 &&
					p.pos+6 < len(p.text) &&
					p.text[p.pos+5] == '\\' && p.text[p.pos+6] == 'u' {
					if p.pos+10 < len(p.text) {
						lowDigits := p.text[p.pos+7 : p.pos+11]
						low, lerr := strconv.ParseUint(lowDigits, 16, 32)
						if lerr == nil && low >= 0xDC00 && low < 0xE000 {
							scalar = rune(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00))
							p.pos += 6
						}
					}
				}
				if scalar >= 0xD800 && scalar < 0xE000 {
					return "", decodeErrf("bad \\u escape")
				}
				out = appendRune(out, scalar)
				p.pos += 5
				continue
			case '"':
				out = append(out, '"')
			case '\\':
				out = append(out, '\\')
			case '/':
				out = append(out, '/')
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			default:
				// A non-ASCII byte after `\` is carried verbatim (with the
				// multi-byte sequence) instead of tearing the UTF-8 rune.
				if e < 0x80 {
					out = append(out, e)
				} else {
					out = append(out, '\\', '\\')
					continue
				}
			}
			p.pos++
		default:
			// Non-ASCII bytes are carried as whole UTF-8 sequences.
			size := utf8SeqLen(c)
			end := p.pos + size
			if end > len(p.text) {
				end = len(p.text)
			}
			seq := p.text[p.pos:end]
			if utf8.ValidString(seq) {
				out = append(out, seq...)
			} else {
				out = append(out, c)
			}
			p.pos = end
		}
	}
}

func appendRune(out []byte, r rune) []byte {
	if r < 0x80 {
		return append(out, byte(r))
	}
	var tmp [4]byte
	n := encodeRune(tmp[:], r)
	return append(out, tmp[:n]...)
}

func encodeRune(b []byte, r rune) int {
	switch {
	case r < 0x80:
		b[0] = byte(r)
		return 1
	case r < 0x800:
		b[0] = 0xC0 | byte(r>>6)
		b[1] = 0x80 | byte(r)&0x3F
		return 2
	case r < 0x10000:
		b[0] = 0xE0 | byte(r>>12)
		b[1] = 0x80 | byte(r>>6)&0x3F
		b[2] = 0x80 | byte(r)&0x3F
		return 3
	default:
		b[0] = 0xF0 | byte(r>>18)
		b[1] = 0x80 | byte(r>>12)&0x3F
		b[2] = 0x80 | byte(r>>6)&0x3F
		b[3] = 0x80 | byte(r)&0x3F
		return 4
	}
}

func utf8SeqLen(first byte) int {
	switch {
	case first < 0x80:
		return 1
	case first >= 0xF0:
		return 4
	case first >= 0xE0:
		return 3
	case first >= 0xC0:
		return 2
	default:
		return 1
	}
}

func (p *jsonParser) literal() (any, error) {
	rest := p.text[p.pos:]
	for _, token := range []string{"true", "false", "null", "-Infinity", "Infinity", "NaN"} {
		if len(rest) > len(token) {
			// Must end at a token boundary (JSON structural char).
			if !isTokenBoundary(rest[len(token)]) {
				continue
			}
			if rest[:len(token)] == token {
				p.pos += len(token)
				return literalValue(token), nil
			}
		} else if len(rest) == len(token) && rest == token {
			p.pos += len(token)
			return literalValue(token), nil
		}
	}
	end := numberEnd(rest)
	if end == 0 {
		preview := rest
		if len(preview) > 20 {
			preview = preview[:20]
		}
		return nil, decodeErrf("unexpected token at offset %d: %q", p.pos, preview)
	}
	raw := rest[:end]
	p.pos += end
	return JSONNumber(raw), nil
}

func literalValue(token string) any {
	switch token {
	case "true":
		return true
	case "false":
		return false
	// serde/encoding equivalents cannot carry NaN/Infinity; degrade to null.
	default:
		return nil
	}
}

func isTokenBoundary(c byte) bool {
	return c == ',' || c == '}' || c == ']' || isJSONWS(c)
}

// numberEnd returns the end offset of a number literal (0 = not a number).
func numberEnd(rest string) int {
	b := []byte(rest)
	i := 0
	if i < len(b) && (b[i] == '-' || b[i] == '+') {
		i++
	}
	digitsStart := i
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	if i == digitsStart {
		return 0
	}
	if i < len(b) && b[i] == '.' {
		fracStart := i + 1
		j := fracStart
		for j < len(b) && b[j] >= '0' && b[j] <= '9' {
			j++
		}
		if j > fracStart {
			i = j
		}
	}
	if i < len(b) && (b[i]|0x20) == 'e' {
		j := i + 1
		if j < len(b) && (b[j] == '-' || b[j] == '+') {
			j++
		}
		expStart := j
		for j < len(b) && b[j] >= '0' && b[j] <= '9' {
			j++
		}
		if j > expStart {
			i = j
		}
	}
	return i
}
