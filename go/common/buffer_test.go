package common

import (
	"strings"
	"testing"
)

func TestBigEndianRoundTrip(t *testing.T) {
	w := NewWriter()
	w.I16(310).U8(12).I16(428).I32(-7).I32(0x12345678).I64(1 << 40)
	r := NewReader(w.Buffer())
	if v, err := r.I16(); err != nil || v != 310 {
		t.Fatalf("I16: %v %v", v, err)
	}
	if v, err := r.U8(); err != nil || v != 12 {
		t.Fatalf("U8: %v %v", v, err)
	}
	if v, err := r.I16(); err != nil || v != 428 {
		t.Fatalf("I16: %v %v", v, err)
	}
	if v, err := r.I32(); err != nil || v != -7 {
		t.Fatalf("I32: %v %v", v, err)
	}
	if v, err := r.I32(); err != nil || v != 0x12345678 {
		t.Fatalf("I32: %v %v", v, err)
	}
	if v, err := r.I64(); err != nil || v != 1<<40 {
		t.Fatalf("I64: %v %v", v, err)
	}
	if r.Remaining() != 0 {
		t.Fatalf("remaining %d", r.Remaining())
	}
}

func TestCodeAbove65535TruncatesLikeJavaShort(t *testing.T) {
	w := NewWriter()
	w.I16(200050)
	r := NewReader(w.Buffer())
	v, err := r.I16()
	if err != nil {
		t.Fatal(err)
	}
	if v != int32(200050&0xFFFF) {
		t.Fatalf("got %d want %d", v, 200050&0xFFFF)
	}
}

func TestStringNoneOnZeroLength(t *testing.T) {
	w := NewWriter()
	tag := "TagA"
	w.String(false, nil).String(true, &tag)
	r := NewReader(w.Buffer())
	got, err := r.String(false)
	if err != nil || got != nil {
		t.Fatalf("String(false): %v %v", got, err)
	}
	got, err = r.String(true)
	if err != nil || got == nil || *got != "TagA" {
		t.Fatalf("String(true): %v %v", got, err)
	}
}

func TestDecimalLongMatchesPython(t *testing.T) {
	vectors := []struct {
		value int64
		hex   string
	}{
		{0, "0000000130"},
		{1, "0000000131"},
		{9, "0000000139"},
		{12345, "000000053132333435"},
		{-1, "000000022d31"},
		{-9223372036854775808, "000000142d39323233333732303336383534373735383038"},
		{2147483648, "0000000a32313437343833363438"},
	}
	for _, v := range vectors {
		w := NewWriter()
		w.DecimalLong(v.value)
		if got := Bytes2String(w.Buffer()); got != strings.ToUpper(v.hex) {
			t.Fatalf("value %d: got %s want %s", v.value, got, v.hex)
		}
		r := NewReader(w.Buffer())
		back, err := r.DecimalLong()
		if err != nil || back != v.value {
			t.Fatalf("value %d: back %d err %v", v.value, back, err)
		}
		if r.Remaining() != 0 {
			t.Fatalf("value %d: remaining %d", v.value, r.Remaining())
		}
	}
}

func TestReaderUnderflowIsError(t *testing.T) {
	r := NewReader([]byte{1, 2})
	if _, err := r.I32(); err == nil {
		t.Fatal("expected underflow error")
	}
	if _, err := NewReader(nil).U8(); err == nil {
		t.Fatal("expected underflow error on empty reader")
	}
}
