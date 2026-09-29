package common

import (
	"bytes"
	"strings"
	"testing"
)

// python3 -c 'import zlib; print(zlib.compress(b"rocketmq-compressed-payload-"*40, 5).hex())'
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
	// 空输入也要成流
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
	// 类型 0（无类型位的历史消息）按 ZLIB 处理
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
		{Lz4Type, "unsupported compression type: 1"},
		{ZstdType, "unsupported compression type: 2"},
		{SnappyType, "unsupported compression type: 4"},
	} {
		// 无第三方依赖：LZ4/ZSTD 必须显式报错，绝不能把压缩字节原样透传
		if _, err := Compress([]byte("data"), c.typ, DefaultCompressLevel); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Compress(%d): %v", c.typ, err)
		}
		if _, err := Decompress([]byte("data"), c.typ); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Decompress(%d): %v", c.typ, err)
		}
	}
	if !IsKind(unsupported(Lz4Type), KindDecode) {
		t.Fatal("unsupported compression must be a decode-kind error")
	}
	if _, err := DecompressBody([]byte("x"), ZlibType); err == nil {
		t.Fatal("decompress of non-zlib bytes must fail")
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
	// 与 Python zlib 输出可互相解开（deflate 实现字节不必一致，流必须一致）
	other, err := ZlibDecompress(mustZlibFixture(t))
	if err != nil || !strings.HasPrefix(string(other), "rocketmq-compressed-") {
		t.Fatal("fixture payload mismatch")
	}
}
