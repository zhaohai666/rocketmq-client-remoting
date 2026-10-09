package common

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// LZ4 block-format codec plus the LZ4 frame wrapper — hand-implemented, zero
// third-party dependencies (repo rule). The RocketMQ wire carries the LZ4
// FRAME format (Java's lz4-java LZ4FrameOutputStream / Python lz4.frame /
// the lz4 CLI all speak the same spec); the bare block layer below is what a
// frame block holds.
//
// Frame layout: magic(4) + FLG(1) + BD(1) + [C.Size(8)] + HC(1)
//   - { BlockSize(4) + block }* + EndMark(4) + [C.Checksum(4)].
//
// Encoder emits FLG = version01 | B.Indep | C.Size, BD = 64KB, HC = byte 1 of
// xxh32(FLG+BD+[C.Size]); a block that fails to shrink is stored raw (bit31).
const (
	lz4Magic         = 0x184d2204
	lz4FrameBlockMax = 65536 // BD=0x40 -> 64KB, matching Java/Python defaults

	lz4MinMatch = 4
	// The last 5 bytes of the input must stay literals (block-format rule);
	// conservative "last match must start >= 12B before the end".
	lz4MFLimit  = 12
	lz4HashLog  = 16
	lz4HashMul  = 2654435761
	lz4EndMark  = 0
	lz4RawFlag  = 0x80000000
	lz4FlgWrite = 0x40 | 0x20 | 0x08 // version=01, B.Indep=1, C.Size=1
	lz4BdWrite  = 0x40               // BlockMaxSize=64KB
)

func lz4Hash(u uint32) uint32 {
	return (u * lz4HashMul) >> (32 - lz4HashLog)
}

// lz4CompressBlock encodes data as one LZ4 block (no frame header).
func lz4CompressBlock(data []byte) []byte {
	n := len(data)
	if n == 0 {
		return nil
	}
	out := make([]byte, 0, n+n/8+64)
	table := make([]int32, 1<<lz4HashLog)
	for i := range table {
		table[i] = -1
	}
	anchor := 0
	i := 0
	readU32 := func(p int) uint32 {
		return binary.LittleEndian.Uint32(data[p:])
	}
	// emitSequence writes token -> literals -> offset -> match-length
	// extension (the on-wire order). matchLen < lz4MinMatch means
	// "literals only" (no match part).
	emitSequence := func(litFrom, litTo, off, matchLen int) {
		litLen := litTo - litFrom
		token := byte(0)
		ll := litLen
		if ll >= 15 {
			token |= 0xf0
			ll -= 15
		} else {
			token |= byte(ll) << 4
			ll = -1
		}
		ml := matchLen - lz4MinMatch
		if ml >= 0 {
			if ml >= 15 {
				token |= 0x0f
				ml -= 15
			} else {
				token |= byte(ml)
				ml = -1
			}
		}
		out = append(out, token)
		if ll >= 0 {
			for ll >= 255 {
				out = append(out, 255)
				ll -= 255
			}
			out = append(out, byte(ll))
		}
		out = append(out, data[litFrom:litTo]...)
		if matchLen >= lz4MinMatch {
			out = append(out, byte(off), byte(off>>8))
			if ml >= 0 {
				for ml >= 255 {
					out = append(out, 255)
					ml -= 255
				}
				out = append(out, byte(ml))
			}
		}
	}

	for i+lz4MinMatch <= n-lz4MFLimit {
		u := readU32(i)
		h := lz4Hash(u)
		ref := table[h]
		table[h] = int32(i)
		if ref < 0 || int(ref) >= i || i-int(ref) > 65535 || readU32(int(ref)) != u {
			i++
			continue
		}
		// Extend the match forward; a match may not run past n-5 (the last
		// 5 bytes stay literals).
		matchLen := lz4MinMatch
		for i+matchLen < n-5 && data[int(ref)+matchLen] == data[i+matchLen] {
			matchLen++
		}
		emitSequence(anchor, i, i-int(ref), matchLen)
		i += matchLen
		anchor = i
	}
	// Final literals (no match after them).
	emitSequence(anchor, n, 0, 0)
	return out
}

// lz4DecompressBlock decodes one LZ4 block. Malformed input errors out and
// never returns partial data (the "unsupported = error, never passthrough" rule).
func lz4DecompressBlock(data []byte) ([]byte, error) {
	n := len(data)
	out := make([]byte, 0, n*4+65536)
	src := 0
	extLen := func(len int) (int, error) {
		if len != 15 {
			return len, nil
		}
		for {
			if src >= n {
				return 0, DecodeError("lz4 decompress: truncated extended length")
			}
			b := int(data[src])
			src++
			len += b
			if b != 255 {
				return len, nil
			}
		}
	}
	for src < n {
		token := int(data[src])
		src++
		litLen, err := extLen(token >> 4)
		if err != nil {
			return nil, err
		}
		if src+litLen > n {
			return nil, DecodeError("lz4 decompress: literals overrun input")
		}
		if litLen > 0 {
			out = append(out, data[src:src+litLen]...)
			src += litLen
		}
		if src >= n {
			break // last-literals sequence ends the block
		}
		if src+2 > n {
			return nil, DecodeError("lz4 decompress: truncated match offset")
		}
		off := int(data[src]) | int(data[src+1])<<8
		src += 2
		if off == 0 {
			return nil, DecodeError("lz4 decompress: zero match offset")
		}
		matchLen, err := extLen(token & 0x0f)
		if err != nil {
			return nil, err
		}
		matchLen += lz4MinMatch
		if off > len(out) {
			return nil, DecodeError("lz4 decompress: match offset before start")
		}
		ref := len(out) - off
		// Byte-wise copy: the format allows overlapping matches.
		for k := 0; k < matchLen; k++ {
			out = append(out, out[ref+k])
		}
	}
	return out, nil
}

// xxh32 is the xxhash32 used by the LZ4 frame header checksum (HC) and
// content checksum. uint32 arithmetic is naturally mod 2^32.
func xxh32(data []byte, seed uint32) uint32 {
	const (
		p1 = 2654435761
		p2 = 2246822519
		p3 = 3266489917
		p4 = 668265263
		p5 = 374761393
	)
	n := len(data)
	i := 0
	var h uint32
	if n >= 16 {
		v1 := seed + p1 + p2
		v2 := seed + p2
		v3 := seed
		v4 := seed - p1
		limit := n - 16
		for {
			// Official XXH32 accumulate round: acc += lane*P2; acc = rotl(acc, 13);
			// acc *= P1 — the rotate comes BEFORE the multiply.
			v1 = bits.RotateLeft32(binary.LittleEndian.Uint32(data[i:])*p2+v1, 13) * p1
			v2 = bits.RotateLeft32(binary.LittleEndian.Uint32(data[i+4:])*p2+v2, 13) * p1
			v3 = bits.RotateLeft32(binary.LittleEndian.Uint32(data[i+8:])*p2+v3, 13) * p1
			v4 = bits.RotateLeft32(binary.LittleEndian.Uint32(data[i+12:])*p2+v4, 13) * p1
			i += 16
			if i > limit {
				break
			}
		}
		h = bits.RotateLeft32(v1, 1) + bits.RotateLeft32(v2, 7) +
			bits.RotateLeft32(v3, 12) + bits.RotateLeft32(v4, 18)
	} else {
		h = seed + p5
	}
	h += uint32(n)
	// Tail rounds: 4-byte groups use P3/P4 (the official XXH32_finalize
	// constants, not P5/P1).
	for ; i+4 <= n; i += 4 {
		h += binary.LittleEndian.Uint32(data[i:]) * p3
		h = bits.RotateLeft32(h, 17) * p4
	}
	for ; i < n; i++ {
		h += uint32(data[i]) * p5
		h = bits.RotateLeft32(h, 11) * p1
	}
	h ^= h >> 15
	h *= p2
	h ^= h >> 13
	h *= p3
	h ^= h >> 16
	return h
}

// lz4CompressFrame produces the LZ4 frame format (Java LZ4FrameOutputStream /
// Python lz4.frame compatible). No block/content checksum on write (the two
// other ports' default); incompressible blocks are stored raw (bit31 set).
func lz4CompressFrame(data []byte) []byte {
	header := []byte{lz4FlgWrite, lz4BdWrite}
	var cs [8]byte
	binary.LittleEndian.PutUint64(cs[:], uint64(len(data)))
	hc := byte((xxh32(append(header, cs[:]...), 0) >> 8) & 0xff)
	parts := [][]byte{{0x04, 0x22, 0x4d, 0x18}, header, cs[:], {hc}}
	n := len(data)
	pos := 0
	for {
		end := pos + lz4FrameBlockMax
		if end > n {
			end = n
		}
		chunk := data[pos:end]
		pos = end
		block := lz4CompressBlock(chunk)
		var sizeField [4]byte
		if len(block) == 0 || len(block) >= len(chunk) {
			// Incompressible: store raw (bit31 set).
			binary.LittleEndian.PutUint32(sizeField[:], lz4RawFlag|uint32(len(chunk)))
			parts = append(parts, sizeField[:], chunk)
		} else {
			binary.LittleEndian.PutUint32(sizeField[:], uint32(len(block)))
			parts = append(parts, sizeField[:], block)
		}
		if pos >= n {
			break
		}
	}
	var endMark [4]byte
	binary.LittleEndian.PutUint32(endMark[:], lz4EndMark)
	parts = append(parts, endMark[:])
	out := make([]byte, 0, 1024)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// lz4DecompressFrame decodes an LZ4 frame; bad magic / bad HC / bad checksum /
// truncated input all error, never return partial data.
func lz4DecompressFrame(data []byte) ([]byte, error) {
	if len(data) < 7 {
		return nil, DecodeError("lz4 frame decompress: input too short")
	}
	if binary.LittleEndian.Uint32(data) != lz4Magic {
		return nil, DecodeError(fmt.Sprintf("lz4 frame decompress: bad magic 0x%x", binary.LittleEndian.Uint32(data)))
	}
	p := 4
	flg := int(data[p])
	p++
	if flg>>6 != 0x1 {
		return nil, DecodeError(fmt.Sprintf("lz4 frame decompress: unsupported version %d", flg>>6))
	}
	blockChecksum := flg&0x10 != 0
	contentSizeFlag := flg&0x08 != 0
	contentChecksum := flg&0x04 != 0
	p++ // BD (decode walks the per-block size fields; BlockMaxSize is not needed)
	contentSize := uint64(0)
	if contentSizeFlag {
		if p+8 > len(data) {
			return nil, DecodeError("lz4 frame decompress: truncated content size")
		}
		contentSize = binary.LittleEndian.Uint64(data[p:])
		p += 8
	}
	// HC = byte 1 of xxh32(FLG+BD+[C.Size]).
	headerEnd := p
	if p >= len(data) {
		return nil, DecodeError("lz4 frame decompress: truncated header checksum")
	}
	hc := data[p]
	p++
	if byte((xxh32(data[4:headerEnd], 0)>>8)&0xff) != hc {
		return nil, DecodeError("lz4 frame decompress: header checksum mismatch")
	}
	var parts []byte
	for {
		if p+4 > len(data) {
			return nil, DecodeError("lz4 frame decompress: truncated block size")
		}
		sizeField := binary.LittleEndian.Uint32(data[p:])
		p += 4
		if sizeField == lz4EndMark {
			break
		}
		isRaw := sizeField&lz4RawFlag != 0
		blockLen := int(sizeField &^ lz4RawFlag)
		if p+blockLen > len(data) {
			return nil, DecodeError("lz4 frame decompress: truncated block data")
		}
		block := data[p : p+blockLen]
		p += blockLen
		if isRaw {
			parts = append(parts, block...)
		} else {
			decoded, err := lz4DecompressBlock(block)
			if err != nil {
				return nil, err
			}
			parts = append(parts, decoded...)
		}
		if blockChecksum {
			p += 4
		}
	}
	if contentChecksum {
		if p+4 > len(data) {
			return nil, DecodeError("lz4 frame decompress: truncated content checksum")
		}
		if xxh32(parts, 0) != binary.LittleEndian.Uint32(data[p:]) {
			return nil, DecodeError("lz4 frame decompress: content checksum mismatch")
		}
		p += 4
	}
	if contentSize != 0 && uint64(len(parts)) != contentSize {
		return nil, DecodeError("lz4 frame decompress: content size mismatch")
	}
	return parts, nil
}
