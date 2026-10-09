package common

import (
	"fmt"
)

// ZSTD entropy coders (FSE + Huffman) — hand-implemented from the zstandard
// format spec, zero third-party dependencies.
//
// The format reads bits in two directions, so two readers are needed:
//   - forward: FSE table descriptions ("NCount" headers), LSB first;
//   - backward: Huffman literal streams, FSE-encoded Huffman weights and the
//     sequences bitstream. Those start just below the "final bit flag" (the
//     highest set bit of the last byte), so decoding may legitimately dip
//     below the start of the stream; bits before the first byte read as 0 and
//     the end position is then validated (a stream that is not exactly
//     consumed is corruption).
const (
	zstdFSEMinTableLog     = 5  // Accuracy_Log = low4bits + 5
	zstdFSEMaxTableLog     = 12 // FSE_TABLELOG_ABSOLUTE_MAX
	zstdHUFMaxTableLog     = 11 // HUF_TABLELOG_MAX, i.e. max Number_of_Bits
	zstdHUFMaxWeights      = 255
	zstdHUFWeightsMaxLog   = 6 // FSE header of a Huffman weight list
	zstdMinLitsFor4Streams = 6
)

// zstdHighBit returns the index of the highest set bit of v, -1 for v == 0.
func zstdHighBit(v int) int {
	r := -1
	for v != 0 {
		v >>= 1
		r++
	}
	return r
}

// zstdReadBitsLE reads numBits (<= 56) bits starting at bit offset bitOff,
// where bit 0 is the LSB of src[0]. Bits outside src read as 0, which is what
// the backward readers need when the state machine dips below the stream.
func zstdReadBitsLE(src []byte, numBits int, bitOff int64) uint64 {
	if numBits <= 0 {
		return 0
	}
	var window uint64
	first := bitOff >> 3
	for i := 0; i < 8; i++ {
		j := first + int64(i)
		if j < 0 {
			continue // below the stream: contributes zero bits
		}
		if j >= int64(len(src)) {
			break // past the stream: also zero bits
		}
		window |= uint64(src[j]) << (8 * i)
	}
	val := window >> uint(bitOff&7)
	if numBits >= 64 {
		return val
	}
	return val & ((uint64(1) << uint(numBits)) - 1)
}

// zstdBitReader is the forward reader used for FSE table headers. Reads past
// the end return 0 bits; callers must check consumed() against the section
// size (the reference decoder does exactly the same).
type zstdBitReader struct {
	src  []byte
	bits int
}

func (br *zstdBitReader) read(n int) uint64 {
	v := zstdReadBitsLE(br.src, n, int64(br.bits))
	br.bits += n
	return v
}

func (br *zstdBitReader) rewind(n int) { br.bits -= n }

func (br *zstdBitReader) align() { br.bits = (br.bits + 7) &^ 7 }

func (br *zstdBitReader) consumed() int { return (br.bits + 7) / 8 }

// zstdFseTable is an FSE decoding table: all three slices are indexed by the
// current state and hold 1<<accuracyLog entries.
type zstdFseTable struct {
	accuracyLog int
	symbols     []byte
	nbBits      []byte
	newBase     []uint16
}

// initBackward reads accuracyLog bits to build the initial state, consuming
// them from the end of the stream.
func (t *zstdFseTable) initBackward(src []byte, off *int64) uint16 {
	*off -= int64(t.accuracyLog)
	return uint16(zstdReadBitsLE(src, t.accuracyLog, *off))
}

// updateBackward advances the state, consuming nbBits[state] bits from the end
// of the stream.
func (t *zstdFseTable) updateBackward(state *uint16, src []byte, off *int64) {
	nb := int(t.nbBits[*state])
	*off -= int64(nb)
	*state = t.newBase[*state] + uint16(zstdReadBitsLE(src, nb, *off))
}

// decodeBackward emits the symbol of state and then advances the state.
func (t *zstdFseTable) decodeBackward(state *uint16, src []byte, off *int64) byte {
	symb := t.symbols[*state]
	t.updateBackward(state, src, off)
	return symb
}

// zstdFseRLETable is the 1-entry table of RLE_Mode: always emits symb, always
// stays in state 0, never consumes a bit.
func zstdFseRLETable(symb byte) *zstdFseTable {
	return &zstdFseTable{symbols: []byte{symb}, nbBits: []byte{0}, newBase: []uint16{0}}
}

// zstdFseBuildTable spreads normalized counters over the table (spread
// allocation, not linear) and pre-computes the per-state bit counts, exactly as
// the reference decoder does.
func zstdFseBuildTable(norm []int16, accuracyLog int) (*zstdFseTable, error) {
	size := 1 << accuracyLog
	t := &zstdFseTable{
		accuracyLog: accuracyLog,
		symbols:     make([]byte, size),
		nbBits:      make([]byte, size),
		newBase:     make([]uint16, size),
	}
	// "less than 1" probabilities (-1) get one cell each, from the end of the
	// table; such a state is a full reset (it reads accuracyLog bits).
	stateNext := make([]int, len(norm))
	highThreshold := size
	for s, c := range norm {
		if c == -1 {
			highThreshold--
			t.symbols[highThreshold] = byte(s)
			stateNext[s] = 1
		} else {
			stateNext[s] = int(c)
		}
	}
	step := (size >> 1) + (size >> 3) + 3
	mask := size - 1
	pos := 0
	for s, c := range norm {
		if c <= 0 {
			continue
		}
		for i := 0; i < int(c); i++ {
			t.symbols[pos] = byte(s)
			for {
				pos = (pos + step) & mask
				if pos < highThreshold {
					break
				}
			}
		}
	}
	if pos != 0 {
		return nil, DecodeError("zstd decompress: corrupted FSE distribution (cells not exactly covered)")
	}
	for u := 0; u < size; u++ {
		symb := t.symbols[u]
		next := stateNext[symb]
		stateNext[symb] = next + 1
		t.nbBits[u] = byte(accuracyLog - zstdHighBit(next))
		t.newBase[u] = uint16((next << t.nbBits[u]) - size)
	}
	return t, nil
}

// zstdFseReadNCount decodes a normalized distribution header. maxSyms bounds
// the number of symbols the table may describe, maxAccuracyLog the largest
// Accuracy_Log accepted for this use. It returns the counters, the accuracy log
// and the number of header bytes consumed.
func zstdFseReadNCount(src []byte, maxSyms, maxAccuracyLog int) ([]int16, int, int, error) {
	br := &zstdBitReader{src: src}
	accuracyLog := int(br.read(4)) + zstdFSEMinTableLog
	if accuracyLog > zstdFSEMaxTableLog || accuracyLog > maxAccuracyLog {
		return nil, 0, 0, DecodeError(fmt.Sprintf("zstd decompress: FSE accuracy log %d too large (max %d)", accuracyLog, maxAccuracyLog))
	}
	norm := make([]int16, maxSyms)
	remaining := 1 << accuracyLog
	syms := 0
	for remaining > 0 && syms < maxSyms {
		// Values from 0..remaining are possible; the low half of the range is
		// encoded with one bit less.
		bits := zstdHighBit(remaining+1) + 1
		val := int(br.read(bits))
		lowerMask := (1 << (bits - 1)) - 1
		threshold := (1 << bits) - 1 - (remaining + 1)
		if val&lowerMask < threshold {
			br.rewind(1)
			val &= lowerMask
		} else if val > lowerMask {
			val -= threshold
		}
		proba := val - 1 // -1 means "less than 1", still counts as one cell
		if proba < 0 {
			remaining -= -proba
		} else {
			remaining -= proba
		}
		norm[syms] = int16(proba)
		syms++
		if proba == 0 {
			// A zero probability is followed by 2-bit repeat flags; 3 means
			// "another group of zeros follows".
			repeat := int(br.read(2))
			for {
				syms += repeat
				if repeat != 3 {
					break
				}
				repeat = int(br.read(2))
			}
		}
	}
	br.align()
	if remaining != 0 {
		return nil, 0, 0, DecodeError("zstd decompress: corrupted FSE distribution (probabilities do not sum to the table size)")
	}
	if syms > maxSyms {
		return nil, 0, 0, DecodeError(fmt.Sprintf("zstd decompress: FSE distribution has %d symbols, max %d", syms, maxSyms))
	}
	if br.consumed() > len(src) {
		return nil, 0, 0, DecodeError("zstd decompress: truncated FSE distribution header")
	}
	return norm, accuracyLog, br.consumed(), nil
}

// zstdFseDecodeTable reads an NCount header and builds the decoding table.
func zstdFseDecodeTable(src []byte, maxSyms, maxAccuracyLog int) (*zstdFseTable, int, error) {
	norm, accuracyLog, consumed, err := zstdFseReadNCount(src, maxSyms, maxAccuracyLog)
	if err != nil {
		return nil, 0, err
	}
	t, err := zstdFseBuildTable(norm, accuracyLog)
	if err != nil {
		return nil, 0, err
	}
	return t, consumed, nil
}

// zstdFseDecodeWeights decodes an FSE-compressed Huffman weight list: a table
// header, then one bitstream decoded with two interleaved states (state1 emits
// the even-indexed symbols, state2 the odd ones).
func zstdFseDecodeWeights(sec []byte) ([]byte, error) {
	t, headerSize, err := zstdFseDecodeTable(sec, zstdHUFMaxWeights, zstdHUFWeightsMaxLog)
	if err != nil {
		return nil, err
	}
	src := sec[headerSize:]
	if len(src) == 0 {
		return nil, DecodeError("zstd decompress: truncated FSE-compressed Huffman weights")
	}
	last := int(src[len(src)-1])
	if last == 0 {
		return nil, DecodeError("zstd decompress: corrupted FSE bitstream (final bit flag missing)")
	}
	off := int64(len(src)*8) - int64(8-zstdHighBit(last))
	state1 := t.initBackward(src, &off)
	state2 := t.initBackward(src, &off)
	weights := make([]byte, 0, 64)
	for {
		if len(weights) == zstdHUFMaxWeights {
			return nil, DecodeError("zstd decompress: Huffman weight list too long")
		}
		weights = append(weights, t.decodeBackward(&state1, src, &off))
		if off < 0 {
			weights = append(weights, t.symbols[state2])
			break
		}
		if len(weights) == zstdHUFMaxWeights {
			return nil, DecodeError("zstd decompress: Huffman weight list too long")
		}
		weights = append(weights, t.decodeBackward(&state2, src, &off))
		if off < 0 {
			weights = append(weights, t.symbols[state1])
			break
		}
	}
	return weights, nil
}

// zstdHufTable is a canonical Huffman decoding table, indexed by the top
// maxBits bits of the stream (symbols with a shorter code own several cells).
type zstdHufTable struct {
	maxBits int
	symbols []byte
	nbBits  []byte
}

// zstdHufBuildFromWeights builds the decode table from a transmitted weight
// list. The weight of the last symbol is not transmitted: it is whatever is
// left of the next power of two.
func zstdHufBuildFromWeights(weights []byte) (*zstdHufTable, error) {
	if len(weights) >= zstdHUFMaxWeights+1 {
		return nil, DecodeError("zstd decompress: too many Huffman weights")
	}
	weightTotal := 0
	rankStats := make([]int, zstdHUFMaxTableLog+1)
	for _, w := range weights {
		if int(w) > zstdHUFMaxTableLog {
			return nil, DecodeError(fmt.Sprintf("zstd decompress: Huffman weight %d exceeds max %d", w, zstdHUFMaxTableLog))
		}
		rankStats[w]++
		if w > 0 {
			weightTotal += 1 << (w - 1)
		}
	}
	if weightTotal == 0 {
		return nil, DecodeError("zstd decompress: Huffman tree has no symbol")
	}
	maxBits := zstdHighBit(weightTotal) + 1
	if maxBits > zstdHUFMaxTableLog {
		return nil, DecodeError(fmt.Sprintf("zstd decompress: Huffman tree needs %d bits, max %d", maxBits, zstdHUFMaxTableLog))
	}
	rest := (1 << maxBits) - weightTotal
	if rest&(rest-1) != 0 {
		return nil, DecodeError("zstd decompress: corrupted Huffman tree weights (last weight is not a clean power of 2)")
	}
	lastWeight := zstdHighBit(rest) + 1
	// The implied weight completes the tree: by construction the number of
	// longest codes (weight 1) must be even and at least 2.
	rankStats[lastWeight]++
	if rankStats[1] < 2 || rankStats[1]&1 != 0 {
		return nil, DecodeError("zstd decompress: corrupted Huffman tree (invalid number of longest weights)")
	}

	// Number_of_Bits = Max_Number_of_Bits + 1 - Weight, 0 for absent symbols.
	numSymbs := len(weights) + 1 // the implied last symbol
	bits := make([]byte, numSymbs)
	rankCount := make([]int, zstdHUFMaxTableLog+1)
	for i, w := range weights {
		if w > 0 {
			bits[i] = byte(maxBits + 1 - int(w))
		}
		rankCount[bits[i]]++
	}
	bits[numSymbs-1] = byte(maxBits + 1 - lastWeight)
	rankCount[bits[numSymbs-1]]++

	size := 1 << maxBits
	t := &zstdHufTable{maxBits: maxBits, symbols: make([]byte, size), nbBits: make([]byte, size)}
	// Codes are assigned per rank in symbol order; rank r starts where the
	// longer ranks stopped.
	rankIdx := make([]int, zstdHUFMaxTableLog+1)
	for i := maxBits; i >= 1; i-- {
		rankIdx[i-1] = rankIdx[i] + rankCount[i]*(1<<(maxBits-i))
		for u := rankIdx[i]; u < rankIdx[i-1]; u++ {
			t.nbBits[u] = byte(i)
		}
	}
	if rankIdx[0] != size {
		return nil, DecodeError("zstd decompress: corrupted Huffman tree (codes do not fill the table)")
	}
	for i := 0; i < numSymbs; i++ {
		if bits[i] == 0 {
			continue
		}
		code := rankIdx[bits[i]]
		length := 1 << (maxBits - int(bits[i]))
		for u := 0; u < length; u++ {
			t.symbols[code+u] = byte(i)
		}
		rankIdx[bits[i]] += length
	}
	return t, nil
}

// zstdDecodeHufTable decodes a Huffman tree description and returns it plus the
// number of bytes it occupies. headerByte >= 128 is a direct 4-bit-weight
// list, otherwise the weights are FSE-compressed into exactly headerByte bytes.
func zstdDecodeHufTable(src []byte) (*zstdHufTable, int, error) {
	if len(src) < 1 {
		return nil, 0, DecodeError("zstd decompress: truncated Huffman tree description")
	}
	header := int(src[0])
	var weights []byte
	consumed := 0
	if header >= 128 {
		numWeights := header - 127
		if numWeights > zstdHUFMaxWeights {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: Huffman tree has %d weights, max %d", numWeights, zstdHUFMaxWeights))
		}
		byteCount := (numWeights + 1) / 2
		if 1+byteCount > len(src) {
			return nil, 0, DecodeError("zstd decompress: truncated Huffman tree description")
		}
		weights = make([]byte, numWeights)
		for i := range weights {
			if i%2 == 0 {
				weights[i] = src[1+i/2] >> 4
			} else {
				weights[i] = src[1+i/2] & 0xf
			}
		}
		consumed = 1 + byteCount
	} else {
		if header < 1 {
			return nil, 0, DecodeError("zstd decompress: empty Huffman tree description")
		}
		if 1+header > len(src) {
			return nil, 0, DecodeError("zstd decompress: truncated FSE-compressed Huffman tree description")
		}
		var err error
		weights, err = zstdFseDecodeWeights(src[1 : 1+header])
		if err != nil {
			return nil, 0, err
		}
		consumed = 1 + header
	}
	t, err := zstdHufBuildFromWeights(weights)
	if err != nil {
		return nil, 0, err
	}
	return t, consumed, nil
}

// zstdHufDecodeStream decodes one backward Huffman bitstream, writing symbols
// into dst. The stream must be consumed exactly.
func zstdHufDecodeStream(t *zstdHufTable, src, dst []byte) (int, error) {
	if len(src) == 0 {
		return 0, DecodeError("zstd decompress: empty Huffman bitstream")
	}
	last := int(src[len(src)-1])
	if last == 0 {
		return 0, DecodeError("zstd decompress: corrupted Huffman bitstream (final bit flag missing)")
	}
	off := int64(len(src)*8) - int64(8-zstdHighBit(last))
	off -= int64(t.maxBits)
	state := uint16(zstdReadBitsLE(src, t.maxBits, off))
	mask := uint32(1<<t.maxBits) - 1
	n := 0
	for off > -int64(t.maxBits) {
		if n == len(dst) {
			return 0, DecodeError("zstd decompress: Huffman bitstream decodes more literals than announced")
		}
		nb := int(t.nbBits[state])
		dst[n] = t.symbols[state]
		n++
		off -= int64(nb)
		rest := zstdReadBitsLE(src, nb, off)
		state = uint16((uint32(state)<<uint(nb) + uint32(rest)) & mask)
	}
	if off != -int64(t.maxBits) {
		return 0, DecodeError("zstd decompress: Huffman bitstream not exactly consumed")
	}
	return n, nil
}

// Codes for literals lengths / match lengths: baseline plus extra bits.
// Offsets need no table, Offset_Value = (1 << Offset_Code) + extra bits.
var zstdLLBase = [36]uint32{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096,
	8192, 16384, 32768, 65536,
}

var zstdLLBits = [36]byte{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12,
	13, 14, 15, 16,
}

var zstdMLBase = [53]uint32{
	3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
	19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
	35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515,
	1027, 2051, 4099, 8195, 16387, 32771, 65539,
}

var zstdMLBits = [53]byte{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16,
}

// Predefined FSE distributions (Predefined_Mode), with their accuracy logs.
const (
	zstdLLDefaultSyms = 36
	zstdLLDefaultLog  = 6
	zstdOFDefaultSyms = 29
	zstdOFDefaultLog  = 5
	zstdMLDefaultSyms = 53
	zstdMLDefaultLog  = 6
	zstdMaxLLCode     = 35
	zstdMaxOFCode     = 31
	zstdMaxMLCode     = 52
	zstdSeqLLMaxLog   = 9
	zstdSeqOFMaxLog   = 8
	zstdSeqMLMaxLog   = 9
)

var zstdLLDefaultNorm = [zstdLLDefaultSyms]int16{
	4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
	-1, -1, -1, -1,
}

var zstdOFDefaultNorm = [zstdOFDefaultSyms]int16{
	1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1,
}

var zstdMLDefaultNorm = [zstdMLDefaultSyms]int16{
	1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1,
	-1, -1, -1, -1, -1,
}
