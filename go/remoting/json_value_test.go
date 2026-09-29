package remoting

import (
	"testing"
)

func TestParseJSONBasics(t *testing.T) {
	v, err := ParseJSON(`{"code":105,"language":"GO","version":515,"opaque":7,"flag":0}`)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if got := m["code"].(JSONNumber).String(); got != "105" {
		t.Fatalf("code = %q", got)
	}
	if got := m["language"].(string); got != "GO" {
		t.Fatalf("language = %q", got)
	}
}

func TestParseJSONWhitespaceAndNesting(t *testing.T) {
	v, err := ParseJSON("{ \"a\" : 1 , \"b\" : [ 1 , 2 ] , \"c\" : { \"d\" : null } }")
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if len(m["b"].([]any)) != 2 {
		t.Fatalf("b = %#v", m["b"])
	}
	if m["c"].(map[string]any)["d"] != nil {
		t.Fatalf("c.d = %#v", m["c"])
	}
}

func TestParseJSONEmptyContainers(t *testing.T) {
	for _, text := range []string{"{}", "[]"} {
		if _, err := ParseJSON(text); err != nil {
			t.Fatalf("%s: %v", text, err)
		}
	}
}

func TestParseJSONBareNumericKeys(t *testing.T) {
	v, err := ParseJSON(`{"brokerAddrs":{0:"127.0.0.1:10911","1":"b"}}`)
	if err != nil {
		t.Fatal(err)
	}
	addrs := v.(map[string]any)["brokerAddrs"].(map[string]any)
	if got := addrs["0"].(string); got != "127.0.0.1:10911" {
		t.Fatalf("addrs[0] = %q", got)
	}
	if got := addrs["1"].(string); got != "b" {
		t.Fatalf("addrs[1] = %q", got)
	}
}

func TestParseJSONObjectAsKey(t *testing.T) {
	text := `{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{"brokerOffset":9}}}`
	v, err := ParseJSON(text)
	if err != nil {
		t.Fatal(err)
	}
	table := v.(map[string]any)["offsetTable"].(map[string]any)
	if len(table) != 1 {
		t.Fatalf("table len = %d", len(table))
	}
	for k, val := range table {
		key, ok := DecodeMapKey(k)
		if !ok {
			t.Fatalf("key %q is not an inline object", k)
		}
		kv := key.(map[string]any)
		if kv["brokerName"].(string) != "b" || kv["queueId"].(JSONNumber).String() != "3" {
			t.Fatalf("decoded key = %#v", kv)
		}
		if val.(map[string]any)["brokerOffset"].(JSONNumber).String() != "9" {
			t.Fatalf("value = %#v", val)
		}
	}
	if _, ok := DecodeMapKey("plain"); ok {
		t.Fatal("plain key must not decode as object")
	}
}

func TestParseJSONTrailingCommas(t *testing.T) {
	obj, err := ParseJSON(`{"a":1,}`)
	if err != nil {
		t.Fatalf("object trailing comma: %v", err)
	}
	if _, ok := obj.(map[string]any)["a"]; !ok {
		t.Fatal("a missing")
	}
	arr, err := ParseJSON(`[1,2,]`)
	if err != nil {
		t.Fatalf("array trailing comma: %v", err)
	}
	if len(arr.([]any)) != 2 {
		t.Fatalf("arr = %#v", arr)
	}
}

func TestParseJSONSpecialLiteralsBecomeNull(t *testing.T) {
	for _, lit := range []string{"NaN", "Infinity", "-Infinity"} {
		v, err := ParseJSON(`{"x":` + lit + `}`)
		if err != nil {
			t.Fatalf("%s: %v", lit, err)
		}
		if v.(map[string]any)["x"] != nil {
			t.Fatalf("%s should degrade to null, got %#v", lit, v)
		}
	}
}

func TestParseJSONBoolLiterals(t *testing.T) {
	v, err := ParseJSON(`{"t":true,"f":false}`)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["t"] != true || m["f"] != false {
		t.Fatalf("m = %#v", m)
	}
	// Token must not bleed into longer identifiers.
	if _, err := ParseJSON(`{"t":truest}`); err == nil {
		t.Fatal("truest should not parse")
	}
}

func TestParseJSONNumbersKeepRawText(t *testing.T) {
	v, err := ParseJSON(`{"max":9223372036854775807,"umax":18446744073709551615,"f":1.5,"e":1e3,"neg":-42}`)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if n, err := m["max"].(JSONNumber).Int64(); err != nil || n != 9223372036854775807 {
		t.Fatalf("max = %v %v", n, err)
	}
	if n, err := m["umax"].(JSONNumber).Uint64(); err != nil || n != 18446744073709551615 {
		t.Fatalf("umax = %v %v", n, err)
	}
	if _, err := m["umax"].(JSONNumber).Int64(); err == nil {
		t.Fatal("umax must overflow int64")
	}
	if f, err := m["f"].(JSONNumber).Float64(); err != nil || f != 1.5 {
		t.Fatalf("f = %v %v", f, err)
	}
	if f, _ := m["e"].(JSONNumber).Float64(); f != 1000 {
		t.Fatalf("e = %v", f)
	}
	if n, _ := m["neg"].(JSONNumber).Int64(); n != -42 {
		t.Fatalf("neg = %v", n)
	}
}

func TestParseJSONStringEscapes(t *testing.T) {
	v, err := ParseJSON(`{"s":"a\nb\tc\\d\"e\/f\bg\fh"}`)
	if err != nil {
		t.Fatal(err)
	}
	want := "a\nb\tc\\d\"e/f\bg\fh"
	if got := v.(map[string]any)["s"].(string); got != want {
		t.Fatalf("s = %q want %q", got, want)
	}
}

func TestParseJSONSurrogatePairs(t *testing.T) {
	v, err := ParseJSON(`{"emoji":"\ud83d\ude00","uh":"a\u00e9"}`)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if got := m["emoji"].(string); got != "😀" {
		t.Fatalf("emoji = %q", got)
	}
	if got := m["uh"].(string); got != "aé" {
		t.Fatalf("uh = %q", got)
	}
	// Lone high surrogate is an error.
	if _, err := ParseJSON(`{"bad":"\ud83d"}`); err == nil {
		t.Fatal("lone surrogate must fail")
	}
}

func TestParseJSONRawUTF8AndBackslashCompat(t *testing.T) {
	// Raw multi-byte UTF-8 inside strings is carried as-is.
	v, err := ParseJSON("{\"t\":\"中文\"}")
	if err != nil {
		t.Fatal(err)
	}
	if got := v.(map[string]any)["t"].(string); got != "中文" {
		t.Fatalf("t = %q", got)
	}
	// fastjson2 quirk: `\` followed by a non-ASCII byte becomes two
	// backslashes plus the verbatim UTF-8 sequence.
	v, err = ParseJSON("\"a\\\xC3\xA9b\"")
	if err != nil {
		t.Fatal(err)
	}
	if got := v.(string); got != "a\\\\éb" {
		t.Fatalf("quirk output = %q", got)
	}
}

func TestParseJSONErrors(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"trailing data", `{"a":1} x`},
		{"missing colon", `{"a"1}`},
		{"missing comma", `[1 2]`},
		{"unexpected token", `tru`},
		{"unterminated string", `"abc`},
		{"unterminated escape", `"a\`},
		{"bad escape digit", `"a\uzzzz"`},
		{"empty input", ""},
		{"bare close", `}`},
	}
	for _, c := range cases {
		if _, err := ParseJSON(c.text); err == nil {
			t.Errorf("%s: expected error for %q", c.name, c.text)
		}
	}
}

func TestDecodeJSONEmptyInput(t *testing.T) {
	v, err := DecodeJSON(nil)
	if err != nil || v != nil {
		t.Fatalf("DecodeJSON(nil) = %v, %v", v, err)
	}
}
