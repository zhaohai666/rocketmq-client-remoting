package common

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// Tests for the ZSTD Compressed-block decoder (zstd.go + zstd_entropy.go).
//
// The fixtures below are frames produced by the official `zstd` CLI, i.e. the
// same encoder family Java's zstd-jni uses on the broker. Before this decoder
// existed the port only understood Raw/RLE blocks, so a message compressed by
// any other port (cpp/libzstd, rust/zstd, csharp/ZstdStream) or by Java was
// undecodable and the consumer simply never saw it. These vectors pin the
// implementation to the reference format.

// `zstd -3 --no-check` over zlibPayload (1040 B): one Compressed block, 4
// Huffman literal streams, FSE-coded sequences.
const zstdCliText3 = "28b52ffd0058250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff7402"

// Same payload, `zstd -9` — identical content plus a Frame_Content_Checksum
// (this is the shape zstd-jni writes when checksums are enabled).
const zstdCliText9Checksum = "28b52ffd0460250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1ff740278aa9792"

// "The quick brown fox jumps over the lazy dog. "*5000 (225000 B), `zstd -19
// --no-check`: a long-range frame whose second block uses Treeless literals
// and Repeat_Mode sequence tables, with offsets beyond one block.
const zstdCliFox19 = "28b52ffd0068bc0100d40254686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e20010085fe87b92a03550000000100e56ee3ffb90602"

// Same payload, `zstd -19` (checksum appended).
const zstdCliFox19Checksum = "28b52ffd0468bc0100d40254686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e20010085fe87b92a03550000000100e56ee3ffb90602c94cb9d8"

// 100000 zero bytes, `zstd -3 --no-check`: an RLE block inside a non-single-
// segment frame (window descriptor 0x58).
const zstdCliZeros3 = "28b52ffd005855000010000001009b8639c002"

// 1024 pseudorandom bytes ((i*137+11)%256), `zstd -1`: a Compressed block
// whose literals are Raw and whose sequence count is zero — the
// "no sequences at all" path.
const zstdCliNoSequences = "28b52ffd044855080004100b941da62fb841ca53dc65ee770089129b24ad36bf48d15ae36cf57e079019a22bb43dc64fd861ea73fc850e9720a932bb44cd56df68f17a038c159e27b039c24bd45de66ff8810a931ca52eb740c952db64ed76ff88119a23ac35be47d059e26bf47d068f18a12ab33cc54ed760e972fb840d961fa831ba43cc55de67f079028b149d26af38c14ad35ce56ef78009921ba42db63fc851da63ec75fe87109922ab34bd46cf58e16af37c058e17a029b23bc44dd65fe871fa830c951ea730b942cb54dd66ef78018a139c25ae37c049d25be46df67f08911aa32cb53ec750d962eb74fd860f9821aa33bc45ce57e069f27b048d169f28b13ac34cd55ee770f982010000fd06aa3505cadad052"

func zstdCliNoSequencesPayload() []byte {
	out := make([]byte, 1024)
	for i := range out {
		out[i] = byte((i*137 + 11) % 256)
	}
	return out
}

func zstdFoxPayload() []byte {
	return bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 5000)
}

func TestZstdDecompressCompressedBlocks(t *testing.T) {
	for _, c := range []struct {
		name    string
		hex     string
		payload func() []byte
	}{
		{"single compressed block", zstdCliText3, func() []byte { return zlibPayload }},
		{"compressed block + checksum", zstdCliText9Checksum, func() []byte { return zlibPayload }},
		{"multi block, repeat tables, long offsets", zstdCliFox19, zstdFoxPayload},
		{"multi block + checksum", zstdCliFox19Checksum, zstdFoxPayload},
		{"rle block, non-single-segment", zstdCliZeros3, func() []byte { return bytes.Repeat([]byte{0}, 100000) }},
		{"compressed block without sequences", zstdCliNoSequences, zstdCliNoSequencesPayload},
	} {
		raw := hexFixture(t, c.hex)
		want := c.payload()
		// Decompress is the entry point the client uses; zstdDecompressFrame
		// is the same code path, called directly so a failure names zstd.
		got, err := zstdDecompressFrame(raw)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: decoded %d bytes, expected %d (first diff at %d)",
				c.name, len(got), len(want), firstDiff(got, want))
		}
		back, err := Decompress(raw, ZstdType)
		if err != nil || !bytes.Equal(back, want) {
			t.Fatalf("%s: Decompress(): %v", c.name, err)
		}
	}
}

func firstDiff(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

func TestZstdCompressedFrameCorruption(t *testing.T) {
	truncated := func() []byte { return hexFixture(t, zstdCliText9Checksum)[:30] }
	badChecksum := func() []byte {
		f := hexFixture(t, zstdCliText9Checksum)
		f[len(f)-16] ^= 0xff // corrupt the content, keep the stored checksum
		return f
	}
	reservedBlock := func() []byte {
		f := hexFixture(t, zstdCliText3)
		f[6] |= 0x06 // block header bits 1-2 := 3 (Reserved)
		return f
	}
	dictionary := func() []byte {
		// magic + descriptor(dictIDFlag=1, Single_Segment) + dictID=1 +
		// one raw block of 2 bytes.
		return []byte{0x28, 0xb5, 0x2f, 0xfd, 0x21, 0x01, 0x02, 0x11, 0x00, 0x00, 'a', 'b'}
	}
	sizeMismatch := func() []byte {
		// Our own single-segment frame (8-byte Frame_Content_Size at offset 5)
		// with the declared size raised by one.
		f := zstdCompressRaw(zlibPayload)
		f[5] ^= 0x01
		return f
	}
	for _, c := range []struct {
		name  string
		frame func() []byte
		wants string
	}{
		{"truncated block payload", truncated, "truncated"},
		{"checksum mismatch", badChecksum, "checksum mismatch"},
		{"reserved block type", reservedBlock, "reserved block type"},
		{"dictionary id", dictionary, "Dictionary_ID"},
		{"regenerated size mismatch", sizeMismatch, "regenerates"},
	} {
		_, err := zstdDecompressFrame(c.frame())
		if err == nil {
			t.Errorf("%s: expected an error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.wants) {
			t.Errorf("%s: error %q does not mention %q", c.name, err, c.wants)
		}
	}
}

// The two block-level modes that inherit state from an earlier block must be
// rejected when a frame opens with them; a hand-built section header is the
// only way to reach that branch.
func TestZstdBlockStateCarryoverGuards(t *testing.T) {
	st := &zstdBlockState{blockMax: zstdBlockMax}
	// Literals_Section_Header: Treeless type (3), single stream (Size_Format 0),
	// zero regenerated and zero compressed bytes.
	if _, _, err := zstdDecodeLiterals(st, []byte{0x03, 0x00, 0x00, 0x00}); err == nil ||
		!strings.Contains(err.Error(), "treeless literals block without a previous Huffman tree") {
		t.Errorf("treeless literals without a previous tree: %v", err)
	}
	// Sequences_Section_Header: 1 sequence, Literals_Lengths_Mode = Repeat (3).
	fresh := &zstdBlockState{blockMax: zstdBlockMax}
	if _, err := zstdDecodeSequences(fresh, []byte{0x01, 0xC0}); err == nil ||
		!strings.Contains(err.Error(), "Repeat_Mode sequence table without a previous table") {
		t.Errorf("Repeat_Mode without a previous table: %v", err)
	}
}

func TestZstdConcatenatedFrames(t *testing.T) {
	// The wire format allows concatenated frames; the decoder must return the
	// concatenation of their contents.
	joined := append(hexFixture(t, zstdCliText3), hexFixture(t, zstdCliZeros3)...)
	got, err := zstdDecompressFrame(joined)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, zlibPayload...), bytes.Repeat([]byte{0}, 100000)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("concatenated frames: got %d bytes, want %d", len(got), len(want))
	}
}

// TestZstdXXH64Vectors pins xxh64 against the Frame_Content_Checksum the
// official CLI writes for the same input (seed 0, low 32 bits compared).
func TestZstdXXH64Vectors(t *testing.T) {
	for _, c := range []struct {
		n    int
		want uint32
	}{
		{0, 0x51d8e999},
		{1, 0xe858bbb7},
		{3, 0xaf428213},
		{7, 0xf739f983},
		{8, 0x308af0c5},
		{12, 0x4aaf88b6},
		{15, 0x9a62e150},
		{16, 0x0f09705e},
		{31, 0x8d2a475e},
		{32, 0x93704537},
		{33, 0x8b643918},
		{40, 0xec1f31fd},
		{100, 0x9aa27121},
		{127, 0x70da3ab2},
		{128, 0x6069acbc},
		{129, 0xcc2c88e1},
		{1000, 0x3d3ded14},
	} {
		data := make([]byte, c.n)
		for i := range data {
			data[i] = byte((i*101 + 7) % 256)
		}
		if got := uint32(zstdXXH64(data, 0)); got != c.want {
			t.Errorf("xxh64(len %d) low32 = %08x, want %08x", c.n, got, c.want)
		}
	}
	// Full 64-bit value of the empty input, the canonical xxh64 test vector.
	if got := zstdXXH64(nil, 0); got != 0xef46db3751d8e999 {
		t.Errorf("xxh64(\"\") = %016x, want ef46db3751d8e999", got)
	}
}

// TestZstdCLICrossCheck runs the reference encoder and decoder against this
// codec both ways, over payloads chosen to hit every block, literal and
// sequence-table mode. Skipped when the zstd CLI is unavailable.
func TestZstdCLICrossCheck(t *testing.T) {
	bin, err := exec.LookPath("zstd")
	if err != nil {
		t.Skip("zstd CLI not installed")
	}
	payloads := map[string][]byte{
		"empty":        {},
		"one byte":     {0x7f},
		"two same":     {0x41, 0x41},
		"tiny":         []byte("ab"),
		"text":         zlibPayload,
		"repeat":       bytes.Repeat([]byte("rocketmq-compressed-payload-"), 5000),
		"zeros":        bytes.Repeat([]byte{0}, 100000),
		"incompressib": incompressibleBytes(200000),
		"mixed":        append(bytes.Repeat([]byte("z"), 130000), incompressibleBytes(50000)...),
	}
	for _, level := range []string{"-1", "-3", "-9", "-19"} {
		for name, payload := range payloads {
			for _, chk := range [][]string{{"--no-check"}, {}} {
				args := append([]string{"-q", "-f", level}, chk...)
				args = append(args, "--stdout")
				cmd := exec.Command(bin, args...)
				cmd.Stdin = bytes.NewReader(payload)
				frame, err := cmd.Output()
				if err != nil {
					t.Fatalf("%s %s: encode: %v", name, level, err)
				}
				got, err := zstdDecompressFrame(frame)
				if err != nil {
					t.Errorf("decode %s level=%s checksum=%v: %v", name, level, len(chk) == 0, err)
					continue
				}
				if !bytes.Equal(got, payload) {
					t.Errorf("decode %s level=%s: %d bytes out, want %d (first diff %d)",
						name, level, len(got), len(payload), firstDiff(got, payload))
					continue
				}
				// The other direction: the reference decoder must accept the
				// Raw/RLE frames this port writes.
				ref := exec.Command(bin, "-q", "-d", "--stdout")
				ref.Stdin = bytes.NewReader(zstdCompressRaw(payload))
				back, err := ref.Output()
				if err != nil {
					t.Errorf("encode %s: zstd -d rejected our frame: %v", name, err)
				} else if !bytes.Equal(back, payload) {
					t.Errorf("encode %s: reference decoder returned the wrong bytes", name)
				}
			}
		}
	}
}
