package common

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// LZ4/ZSTD tests: round trips + hardcoded cross-check vectors produced by the
// real `lz4` / `zstd` CLIs (same wire formats as Java lz4-java / zstd-jni on
// the broker), so the hand-rolled codecs are pinned to the official formats.

// `printf 'rocketmq-compressed-payload-'*40 | lz4 -B4 --no-frame-crc -f -z`
// — no content size, no checksums (FLG=0x60).
const lz4CliFixtureNoSize = "04224d186040822b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffffffff30506c6f61642d00000000"

// Same payload, `lz4 -B4 -f -z` (content checksum appended, FLG=0x64).
const lz4CliFixtureWithChecksum = "04224d186440a72b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c00ffffffff30506c6f61642d00000000c9dd3f36"

// `lz4 -B4 --no-frame-crc -f -z` over "The quick brown fox jumps over the lazy
// dog. "*5000 (225000 bytes > 3x 64KB) — multi-block frame.
const lz4CliFixtureMultiBlock = "04224d1860408238010000ff1e54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e202d00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffbb50726f776e2038010000ff1e666f78206a756d7073206f76657220746865206c617a7920646f672e2054686520717569636b2062726f776e202d00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffbb50766572207438010000ff1e6865206c617a7920646f672e2054686520717569636b2062726f776e20666f78206a756d7073206f76657220742d00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffbb502e20546865a7000000ff1e20717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e205468652d00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1250646f672e2000000000"

func lz4CliMultiBlockPayload() []byte {
	return bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 5000)
}

func hexFixture(t *testing.T, hex string) []byte {
	t.Helper()
	raw := String2Bytes(hex)
	if raw == nil {
		t.Fatal("bad fixture hex")
	}
	return raw
}

func TestLz4DecompressExternalFixtures(t *testing.T) {
	if !bytes.Equal(mustDecompress(t, lz4CliFixtureNoSize, Lz4Type), zlibPayload) {
		t.Fatal("no-size fixture payload mismatch")
	}
	// The content-checksum fixture also validates our xxh32 against the
	// CLI's checksum (header HC + trailing content checksum).
	if !bytes.Equal(mustDecompress(t, lz4CliFixtureWithChecksum, Lz4Type), zlibPayload) {
		t.Fatal("checksum fixture payload mismatch")
	}
	if !bytes.Equal(mustDecompress(t, lz4CliFixtureMultiBlock, Lz4Type), lz4CliMultiBlockPayload()) {
		t.Fatal("multi-block fixture payload mismatch")
	}
}

func mustDecompress(t *testing.T, fixtureHex string, typ int32) []byte {
	t.Helper()
	out, err := Decompress(hexFixture(t, fixtureHex), typ)
	if err != nil {
		t.Fatalf("Decompress(%d): %v", typ, err)
	}
	return out
}

func TestLz4RoundTrip(t *testing.T) {
	for _, c := range []struct {
		name  string
		input []byte
	}{
		{"empty", nil},
		{"tiny", []byte("ab")},
		{"small", []byte("hello hello hello hello hello!")},
		{"repeating", zlibPayload},
		{"zeros", bytes.Repeat([]byte{0}, 200000)},
		{"incompressible", incompressibleBytes(300000)},
		{"just-over-block", bytes.Repeat([]byte("x"), 65537)},
	} {
		enc := lz4CompressFrame(c.input)
		if len(enc) < 4 || binary.LittleEndian.Uint32(enc) != lz4Magic {
			t.Fatalf("%s: bad frame magic", c.name)
		}
		if enc[4] != lz4FlgWrite || enc[5] != lz4BdWrite {
			t.Fatalf("%s: unexpected FLG/BD 0x%X/0x%X", c.name, enc[4], enc[5])
		}
		back, err := lz4DecompressFrame(enc)
		if err != nil || !bytes.Equal(back, c.input) {
			t.Fatalf("%s: round trip failed: %v", c.name, err)
		}
	}
}

// incompressibleBytes: deterministic pseudo-random-ish bytes (no stdlib math/rand
// seeding concerns; simple LCG).
func incompressibleBytes(n int) []byte {
	out := make([]byte, n)
	x := uint32(123456789)
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = byte(x >> 24)
	}
	return out
}

func TestLz4BlockOverlappingMatch(t *testing.T) {
	// Classic LZ4 overlap case: a long run encoded via short offsets.
	input := bytes.Repeat([]byte{'A', 'B'}, 500)
	enc := lz4CompressFrame(input)
	back, err := lz4DecompressFrame(enc)
	if err != nil || !bytes.Equal(back, input) {
		t.Fatalf("overlap round trip failed: %v", err)
	}
	if len(enc) >= len(input) {
		t.Fatalf("repetitive input must shrink: %d -> %d", len(input), len(enc))
	}
}

func TestLz4MalformedFramesError(t *testing.T) {
	good := lz4CompressFrame(zlibPayload)
	bad := func(name string, mutate func([]byte) []byte) {
		t.Helper()
		if _, err := lz4DecompressFrame(mutate(append([]byte(nil), good...))); err == nil {
			t.Fatalf("%s must error", name)
		}
	}
	if _, err := lz4DecompressFrame([]byte{0x04, 0x22}); err == nil {
		t.Fatal("short input must error")
	}
	bad("bad magic", func(b []byte) []byte { b[2] = 0x00; return b })
	bad("bad version", func(b []byte) []byte { b[4] = 0x80 | b[4]&0x3f; return b })
	bad("bad header checksum", func(b []byte) []byte { b[13] ^= 0xff; return b })
	bad("truncated block", func(b []byte) []byte { return b[:len(b)-8] })
	bad("truncated checksum", func(b []byte) []byte {
		// 用带内容校验和的帧验证截断
		f := hexFixture(t, lz4CliFixtureWithChecksum)
		return f[:len(f)-2]
	})
	if _, err := lz4DecompressBlock([]byte{0xf0, 0xff, 0x01, 1, 2, 3}); err == nil {
		t.Fatal("block with literals overrun must error")
	}
}

func TestZstdDecodeExternalFixtures(t *testing.T) {
	// `printf ab | zstd -3 -f --no-check` — single RAW block, 1-byte FCS.
	out, err := Decompress(hexFixture(t, "28b52ffd20021100006162"), ZstdType)
	if err != nil || string(out) != "ab" {
		t.Fatalf("raw-block fixture: %q %v", out, err)
	}
	// A real zstd frame over 100000 zero bytes (single-segment, 8-byte FCS)
	// carries an RLE block; see zstdCliZeros3 for the multi-block form.
	zeros, err := Decompress(hexFixture(t, "28b52ffda0a086010055000010000001009b8639c002"), ZstdType)
	if err != nil {
		t.Fatalf("rle-block fixture: %v", err)
	}
	if len(zeros) != 100000 || !bytes.Equal(zeros, bytes.Repeat([]byte{0}, 100000)) {
		t.Fatalf("rle-block fixture: got %d bytes", len(zeros))
	}
}

func TestZstdRoundTrip(t *testing.T) {
	for _, c := range []struct {
		name  string
		input []byte
	}{
		{"empty", nil},
		{"tiny", []byte("ab")},
		{"text", zlibPayload},
		{"rle", bytes.Repeat([]byte{0xAB}, 100000)},
		{"rle-multi-block", bytes.Repeat([]byte{7}, 300000)},
		{"incompressible", incompressibleBytes(200000)},
		{"mixed", append(bytes.Repeat([]byte("z"), 130000), incompressibleBytes(50000)...)},
	} {
		enc := zstdCompressRaw(c.input)
		if len(enc) < 13 || binary.LittleEndian.Uint32(enc) != zstdMagic {
			t.Fatalf("%s: bad frame magic", c.name)
		}
		if enc[4] != 0xe0 { // FCS_Field_Size=8 + Single_Segment
			t.Fatalf("%s: unexpected header descriptor 0x%X", c.name, enc[4])
		}
		if got := binary.LittleEndian.Uint64(enc[5:13]); got != uint64(len(c.input)) {
			t.Fatalf("%s: content size %d, want %d", c.name, got, len(c.input))
		}
		back, err := zstdDecompressFrame(enc)
		if err != nil || !bytes.Equal(back, c.input) {
			t.Fatalf("%s: round trip failed: %v", c.name, err)
		}
	}
	// RLE payload must actually collapse to a tiny frame.
	enc := zstdCompressRaw(bytes.Repeat([]byte{0}, 100000))
	if len(enc) > 32 {
		t.Fatalf("RLE frame should be tiny, got %d bytes", len(enc))
	}
}

func TestZstdMalformedFramesError(t *testing.T) {
	if _, err := zstdDecompressFrame([]byte{0x28, 0xb5}); err == nil {
		t.Fatal("short input must error")
	}
	if _, err := zstdDecompressFrame([]byte{0x00, 0x00, 0x00, 0x00}); err == nil {
		t.Fatal("bad magic must error")
	}
	// RLE block (last, type=1, size=1) with its payload byte missing.
	if _, err := zstdDecompressFrame(hexFixture(t, "28b52ffd20020b0000")); err == nil {
		t.Fatal("truncated rle block must error")
	}
	// Raw block (last, type=0, size=2) with only 1 of 2 payload bytes.
	if _, err := zstdDecompressFrame(hexFixture(t, "28b52ffd200211000061")); err == nil {
		t.Fatal("truncated raw block must error")
	}
}

func TestXxh32OfficialVectors(t *testing.T) {
	// Official xxHash spec vectors (short path) ...
	for _, c := range []struct {
		in   string
		want uint32
	}{
		{"", 0x02cc5d05},
		{"a", 0x550d7456},
		{"abc", 0x32d153ff},
	} {
		if got := xxh32([]byte(c.in), 0); got != c.want {
			t.Fatalf("xxh32(%q) = %08x, want %08x", c.in, got, c.want)
		}
	}
	// ... and the stripe path (>=16B), pinned against the lz4 CLI's content
	// checksum of zlibPayload ("lz4 -B4 -f -z" -> trailing C.Checksum).
	if got := xxh32(zlibPayload, 0); got != 0x363fddc9 {
		t.Fatalf("xxh32(zlibPayload) = %08x, want 363fddc9 (lz4 CLI content checksum)", got)
	}
}

func TestCompressCrossTypeDispatch(t *testing.T) {
	// 全类型矩阵：同一 payload 三种类型各自成环，且输出互不相同（算法不同）。
	seen := map[string]bool{}
	for _, typ := range []int32{ZlibType, Lz4Type, ZstdType} {
		enc, err := Compress(zlibPayload, typ, DefaultCompressLevel)
		if err != nil {
			t.Fatal(err)
		}
		back, err := Decompress(enc, typ)
		if err != nil || !bytes.Equal(back, zlibPayload) {
			t.Fatalf("type %d round trip: %v", typ, err)
		}
		if seen[string(enc[:4])] {
			t.Fatalf("type %d header duplicates another type", typ)
		}
		seen[string(enc[:4])] = true
	}
}
