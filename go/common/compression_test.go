package common

import (
	"bytes"
	"strings"
	"testing"
)

// Reference fixture: zlib.compress(b"rocketmq-compressed-payload-"*40, level 5), hex-encoded.
const zlibFixture47 = "785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a951b951b951b95a3400e0058e0b9f0"

var zlibPayload = bytes.Repeat([]byte("rocketmq-compressed-payload-"), 40)

func mustZlibFixture(t *testing.T) []byte {
	t.Helper()
	raw := String2Bytes(zlibFixture47)
	if raw == nil {
		t.Fatal("bad zlib fixture hex")
	}
	return raw
}

func TestZlibDecompressExternalFixture(t *testing.T) {
	raw := mustZlibFixture(t)
	if raw[0] != 0x78 {
		t.Fatalf("fixture must start with the zlib header byte, got 0x%X", raw[0])
	}
	got, err := ZlibDecompress(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, zlibPayload) {
		t.Fatalf("decompressed %d bytes, want %d", len(got), len(zlibPayload))
	}
}

func TestZlibRoundTripAndLevel(t *testing.T) {
	compressed, err := ZlibCompress(zlibPayload, DefaultCompressLevel)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ZlibDecompress(compressed)
	if err != nil || !bytes.Equal(back, zlibPayload) {
		t.Fatalf("round trip failed: %v", err)
	}
	// Empty input must also produce a valid stream
	empty, err := ZlibCompress(nil, DefaultCompressLevel)
	if err != nil {
		t.Fatal(err)
	}
	if back, err = ZlibDecompress(empty); err != nil || len(back) != 0 {
		t.Fatalf("empty round trip: %d %v", len(back), err)
	}
	if _, err := ZlibCompress([]byte("x"), 10); err == nil {
		t.Fatal("level 10 must be rejected")
	}
	if _, err := ZlibCompress([]byte("x"), -1); err == nil {
		t.Fatal("level -1 must be rejected")
	}
}

func TestZlibTruncatedStreamIsAnError(t *testing.T) {
	raw := mustZlibFixture(t)
	if _, err := ZlibDecompress(raw[:20]); err == nil {
		t.Fatal("truncated stream must error, never return partial data")
	}
	if _, err := ZlibDecompress([]byte{0x00, 0x01, 0x02}); err == nil {
		t.Fatal("garbage must error")
	}
}

func TestCompressDispatchAndUnsupportedTypes(t *testing.T) {
	// Type 0 (legacy messages without a type bit) is handled as ZLIB
	got, err := Compress([]byte("data"), 0, DefaultCompressLevel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decompress(got, 0); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		typ  int32
		want string
	}{
		{SnappyType, "unsupported compression type: 4"},
	} {
		// No third-party dependency: Snappy must error explicitly; never pass the compressed bytes through unchanged
		if _, err := Compress([]byte("data"), c.typ, DefaultCompressLevel); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Compress(%d): %v", c.typ, err)
		}
		if _, err := Decompress([]byte("data"), c.typ); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Decompress(%d): %v", c.typ, err)
		}
	}
	if !IsKind(unsupported(SnappyType), KindDecode) {
		t.Fatal("unsupported compression must be a decode-kind error")
	}
	if _, err := DecompressBody([]byte("x"), ZlibType); err == nil {
		t.Fatal("decompress of non-zlib bytes must fail")
	}
	// LZ4/ZSTD are now supported (hand-rolled codec) and go through the full Compress/Decompress path
	for _, c := range []int32{Lz4Type, ZstdType} {
		enc, err := Compress(zlibPayload, c, DefaultCompressLevel)
		if err != nil {
			t.Fatal(err)
		}
		back, err := Decompress(enc, c)
		if err != nil || !bytes.Equal(back, zlibPayload) {
			t.Fatalf("type %d round trip failed: %v", c, err)
		}
	}
}

func TestCompressionTypeName(t *testing.T) {
	for _, c := range []struct {
		typ  int32
		want string
	}{
		{Lz4Type, "LZ4"},
		{ZstdType, "ZSTD"},
		{ZlibType, "ZLIB"},
		{0, "ZLIB"},
	} {
		if got, ok := CompressionTypeName(c.typ); !ok || got != c.want {
			t.Fatalf("type %d name %q ok=%v", c.typ, got, ok)
		}
	}
	if _, ok := CompressionTypeName(9); ok {
		t.Fatal("unknown type has no name")
	}
}

func TestZlibOutputIsRFC1950(t *testing.T) {
	compressed, err := ZlibCompress(zlibPayload, 5)
	if err != nil {
		t.Fatal(err)
	}
	if compressed[0] != 0x78 {
		t.Fatalf("missing zlib header byte, got 0x%X", compressed[0])
	}
	// Interoperable with standard zlib output (the deflate byte layout need not match, but the stream must)
	other, err := ZlibDecompress(mustZlibFixture(t))
	if err != nil || !strings.HasPrefix(string(other), "rocketmq-compressed-") {
		t.Fatal("fixture payload mismatch")
	}
}
