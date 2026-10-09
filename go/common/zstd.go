package common

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Minimal ZSTD frame codec — hand-implemented, zero third-party dependencies.
//
// ENCODE = a legal zstd frame built from RAW blocks only (plus RLE for long
// runs of one byte): magic + frame header (single-segment, 8-byte content
// size) + { 3-byte block header + block }*. Any standard decoder (zstd-jni on
// the broker, the zstd CLI) accepts it; the payload is stored uncompressed.
//
// DECODE is a complete frame reader: Raw / RLE / Compressed blocks, the whole
// frame header (all Frame_Content_Size field sizes, Single_Segment,
// Window_Descriptor, Dictionary_ID sizes, Content Checksum), skippable frames,
// concatenated frames, Huffman-coded literals (1 or 4 streams), FSE-coded
// sequences and the repeated-offset rules, plus the xxh64 Content Checksum.
// Anything unsupported or malformed returns an error — the cross-port
// "unsupported = throw, never passthrough" rule. Dictionary-compressed frames
// are the one deliberate exception: they cannot be decoded without the
// dictionary, so they error out instead of returning garbage.
const (
	zstdMagic               = 0xfd2fb528
	zstdSkippableMagicMin   = 0x184d2a50
	zstdSkippableMagicMax   = 0x184d2a5f
	zstdBlockMax            = 128 * 1024 // ZSTD_BLOCKSIZE_MAX
	zstdBlockTypeRaw        = 0
	zstdBlockTypeRLE        = 1
	zstdBlockTypeCompressed = 2
	zstdWindowLogMax        = 31 // ZSTD_WINDOWLOG_MAX (64-bit build)

	// Literals_Section_Header block types.
	zstdLitRaw        = 0
	zstdLitRLE        = 1
	zstdLitCompressed = 2
	zstdLitTreeless   = 3

	// Sequences_Section_Header compression modes.
	zstdSeqPredefined = 0
	zstdSeqRLE        = 1
	zstdSeqFSE        = 2
	zstdSeqRepeat     = 3
)

// zstdStartOffsets is the initial repeated-offset history of every frame.
var zstdStartOffsets = [3]int64{1, 4, 8}

func zstdBlockHeader(last bool, blockType, size int) [3]byte {
	sizeBits := size << 3
	var h [3]byte
	h[0] = byte(sizeBits)
	h[1] = byte(sizeBits >> 8)
	h[2] = byte(sizeBits >> 16)
	h[0] |= byte(blockType) << 1
	if last {
		h[0] |= 0x01
	}
	return h
}

func zstdIsRunOfOneByte(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	b0 := data[0]
	for _, b := range data[1:] {
		if b != b0 {
			return false
		}
	}
	return true
}

// zstdCompressRaw builds a minimal valid ZSTD frame: single-segment header
// with an 8-byte Frame_Content_Size, then RAW/RLE blocks.
func zstdCompressRaw(data []byte) []byte {
	parts := make([][]byte, 0, 8)
	var magic [4]byte
	binary.LittleEndian.PutUint32(magic[:], zstdMagic)
	parts = append(parts, magic[:])
	// Frame header descriptor: FCS_Field_Size=8 (flag 3), Single_Segment=1.
	parts = append(parts, []byte{0xe0 | 0x20})
	var fcs [8]byte
	binary.LittleEndian.PutUint64(fcs[:], uint64(len(data)))
	parts = append(parts, fcs[:])
	pos := 0
	for pos < len(data) || len(data) == 0 {
		end := pos + zstdBlockMax
		if end > len(data) {
			end = len(data)
		}
		chunk := data[pos:end]
		last := pos+len(chunk) >= len(data)
		if len(chunk) >= 2 && zstdIsRunOfOneByte(chunk) {
			// RLE block: 1 payload byte repeated len(chunk) times.
			h := zstdBlockHeader(last, zstdBlockTypeRLE, len(chunk))
			parts = append(parts, h[:], chunk[:1])
		} else {
			h := zstdBlockHeader(last, zstdBlockTypeRaw, len(chunk))
			parts = append(parts, h[:], chunk)
		}
		pos += len(chunk)
		if len(data) == 0 {
			break
		}
	}
	out := make([]byte, 0, 32)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// zstdDecompressFrame decodes a ZSTD stream: one or more concatenated frames,
// skippable frames included. Compressed blocks are decoded for real (Huffman
// literals + FSE sequences), frames carrying a Content Checksum are verified,
// and anything unsupported returns an error — the caller turns that into
// "decode failed", never into handing back compressed bytes.
func zstdDecompressFrame(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, DecodeError("zstd decompress: input too short")
	}
	out := make([]byte, 0, len(data))
	for p := 0; p < len(data); {
		magic := binary.LittleEndian.Uint32(data[p:])
		switch {
		case magic == zstdMagic:
			n, err := zstdDecodeFrameInto(data[p:], &out)
			if err != nil {
				return nil, err
			}
			p += n
		case magic >= zstdSkippableMagicMin && magic <= zstdSkippableMagicMax:
			if p+8 > len(data) {
				return nil, DecodeError("zstd decompress: truncated skippable frame header")
			}
			size := uint64(binary.LittleEndian.Uint32(data[p+4:]))
			if size > uint64(len(data)-p-8) {
				return nil, DecodeError("zstd decompress: truncated skippable frame")
			}
			p += 8 + int(size)
		default:
			return nil, DecodeError(fmt.Sprintf("zstd decompress: bad magic 0x%x at offset %d", magic, p))
		}
	}
	return out, nil
}

// zstdDecodeFrameInto decodes one frame, appending its content to *out, and
// returns the number of input bytes consumed.
func zstdDecodeFrameInto(data []byte, out *[]byte) (int, error) {
	if len(data) < 5 {
		return 0, DecodeError("zstd decompress: truncated frame header")
	}
	magic := binary.LittleEndian.Uint32(data)
	if magic != zstdMagic {
		return 0, DecodeError(fmt.Sprintf("zstd decompress: bad magic 0x%x", magic))
	}
	desc := int(data[4])
	if desc&0x08 != 0 {
		return 0, DecodeError("zstd decompress: reserved bit set in Frame_Header_Descriptor")
	}
	fcsFlag := (desc >> 6) & 0x3
	singleSegment := desc&0x20 != 0
	checksumFlag := desc&0x04 != 0
	dictIdFlag := desc & 0x3

	p := 5
	var windowSize uint64
	if !singleSegment {
		// Window_Descriptor: Exponent (5 bits) + Mantissa (3 bits).
		wd := int(data[p])
		p++
		windowLog := (wd >> 3) + 10
		if windowLog > zstdWindowLogMax {
			return 0, DecodeError(fmt.Sprintf("zstd decompress: window log %d too large (max %d)", windowLog, zstdWindowLogMax))
		}
		base := uint64(1) << uint(windowLog)
		windowSize = base + (base>>3)*uint64(wd&7)
	}

	dictIdSizes := [...]int{0, 1, 2, 4}
	dictIdSize := dictIdSizes[dictIdFlag]
	if p+dictIdSize > len(data) {
		return 0, DecodeError("zstd decompress: truncated frame header")
	}
	if dictIdSize > 0 && zstdReadLE(data[p:], dictIdSize) != 0 {
		return 0, DecodeError(fmt.Sprintf("zstd decompress: frames using Dictionary_ID %d are not supported (no dictionary)", zstdReadLE(data[p:], dictIdSize)))
	}
	p += dictIdSize

	fcsSizes := [...]int{0, 2, 4, 8}
	if singleSegment {
		fcsSizes = [...]int{1, 2, 4, 8}
	}
	fcsSize := fcsSizes[fcsFlag]
	if p+fcsSize > len(data) {
		return 0, DecodeError("zstd decompress: truncated frame header")
	}
	var contentSize uint64
	hasContentSize := fcsSize > 0
	if hasContentSize {
		contentSize = zstdReadLE(data[p:], fcsSize)
		if fcsSize == 2 {
			contentSize += 256 // 2-byte field is the offset-of-256 encoding
		}
	}
	p += fcsSize
	if singleSegment {
		windowSize = contentSize
	}

	blockMax := windowSize
	if blockMax > zstdBlockMax {
		blockMax = zstdBlockMax
	}
	st := &zstdBlockState{
		blockMax: int(blockMax),
		rep:      zstdStartOffsets,
	}
	frameStart := len(*out)
	for {
		if p+3 > len(data) {
			return 0, DecodeError("zstd decompress: truncated block header")
		}
		h := int(data[p]) | int(data[p+1])<<8 | int(data[p+2])<<16
		p += 3
		last := h&1 == 1
		blockType := (h >> 1) & 0x3
		size := h >> 3
		switch blockType {
		case zstdBlockTypeRaw, zstdBlockTypeRLE:
			if uint64(size) > blockMax {
				return 0, DecodeError(fmt.Sprintf("zstd decompress: block regenerates %d bytes, max %d", size, blockMax))
			}
			if blockType == zstdBlockTypeRaw {
				if size > len(data)-p {
					return 0, DecodeError("zstd decompress: truncated raw block")
				}
				*out = append(*out, data[p:p+size]...)
				p += size
			} else {
				if len(data)-p < 1 {
					return 0, DecodeError("zstd decompress: truncated rle block")
				}
				*out = append(*out, zstdRepeatByte(data[p], size)...)
				p++
			}
		case zstdBlockTypeCompressed:
			if size > len(data)-p {
				return 0, DecodeError("zstd decompress: truncated compressed block")
			}
			if err := zstdDecodeCompressedBlock(st, data[p:p+size], out, frameStart, windowSize); err != nil {
				return 0, err
			}
			p += size
		default:
			return 0, DecodeError("zstd decompress: reserved block type 3 (not a block in this format version)")
		}
		if last {
			break
		}
	}

	if produced := len(*out) - frameStart; hasContentSize && uint64(produced) != contentSize {
		return 0, DecodeError(fmt.Sprintf("zstd decompress: frame regenerates %d bytes, header announces %d", produced, contentSize))
	}
	if checksumFlag {
		if len(data)-p < 4 {
			return 0, DecodeError("zstd decompress: truncated content checksum")
		}
		want := binary.LittleEndian.Uint32(data[p:])
		if got := uint32(zstdXXH64((*out)[frameStart:], 0)); got != want {
			return 0, DecodeError(fmt.Sprintf("zstd decompress: content checksum mismatch (frame has 0x%08x, content hashes to 0x%08x)", want, got))
		}
		p += 4
	}
	return p, nil
}

// zstdReadLE reads a little-endian field of 1, 2, 4 or 8 bytes.
func zstdReadLE(b []byte, size int) uint64 {
	switch size {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// zstdBlockState is everything a compressed block inherits from the previous
// blocks of the same frame: the offset ceiling, the repeated-offset history,
// the Huffman tree for literals and the three FSE tables for the sequences.
// It is created fresh for every frame.
type zstdBlockState struct {
	blockMax   int
	rep        [3]int64
	huf        *zstdHufTable
	ll, of, ml *zstdFseTable
	litEntropy bool // a Huffman tree is available (set by Compressed literals)
	fseEntropy bool // sequence tables are available (Repeat_Mode)
}

// zstdSequence is a decoded literal-length / offset / match-length triple.
// offsetValue is the raw Offset_Value, before the repeated-offset rules.
type zstdSequence struct {
	litLength   int
	offsetValue int64
	matchLength int
}

// zstdDecodeCompressedBlock decodes one Compressed_Block: literals, then
// sequences, then the two are combined into output.
func zstdDecodeCompressedBlock(st *zstdBlockState, src []byte, out *[]byte, frameStart int, windowSize uint64) error {
	if len(src) < 2 {
		return DecodeError("zstd decompress: compressed block too small")
	}
	blockStart := len(*out)
	literals, pos, err := zstdDecodeLiterals(st, src)
	if err != nil {
		return err
	}
	seqs, err := zstdDecodeSequences(st, src[pos:])
	if err != nil {
		return err
	}
	if err := zstdExecuteSequences(st, literals, seqs, out, frameStart, windowSize); err != nil {
		return err
	}
	if regenerated := len(*out) - blockStart; regenerated > st.blockMax {
		return DecodeError(fmt.Sprintf("zstd decompress: block regenerates %d bytes, max %d", regenerated, st.blockMax))
	}
	return nil
}

// zstdDecodeLiterals decodes the Literals_Section_Header and the literals that
// follow it, returning the literals and the section size in bytes.
func zstdDecodeLiterals(st *zstdBlockState, src []byte) ([]byte, int, error) {
	blockType := int(src[0] & 3)
	sizeFormat := int((src[0] >> 2) & 3)
	switch blockType {
	case zstdLitRaw, zstdLitRLE:
		var lhSize, litSize int
		switch sizeFormat {
		case 0, 2:
			lhSize, litSize = 1, int(src[0])>>3
		case 1:
			if len(src) < 2 {
				return nil, 0, DecodeError("zstd decompress: truncated literals section header")
			}
			lhSize, litSize = 2, int(binary.LittleEndian.Uint16(src))>>4
		default:
			if len(src) < 3 {
				return nil, 0, DecodeError("zstd decompress: truncated literals section header")
			}
			lhSize = 3
			litSize = (int(src[0]) | int(src[1])<<8 | int(src[2])<<16) >> 4
		}
		if litSize > st.blockMax {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: %d literals exceed the block size limit %d", litSize, st.blockMax))
		}
		literals := make([]byte, litSize)
		if blockType == zstdLitRaw {
			if litSize > len(src)-lhSize {
				return nil, 0, DecodeError("zstd decompress: truncated raw literals")
			}
			copy(literals, src[lhSize:lhSize+litSize])
		} else {
			if len(src)-lhSize < 1 {
				return nil, 0, DecodeError("zstd decompress: truncated rle literals")
			}
			for i := range literals {
				literals[i] = src[lhSize]
			}
		}
		return literals, lhSize + len(literals), nil

	case zstdLitCompressed, zstdLitTreeless:
		if len(src) < 4 {
			return nil, 0, DecodeError("zstd decompress: truncated literals section header")
		}
		lhc := binary.LittleEndian.Uint32(src)
		singleStream := sizeFormat == 0
		var lhSize, litSize, litCSize int
		switch sizeFormat {
		case 0, 1:
			lhSize = 3
			litSize = int((lhc >> 4) & 0x3FF)
			litCSize = int((lhc >> 14) & 0x3FF)
		case 2:
			lhSize = 4
			litSize = int((lhc >> 4) & 0x3FFF)
			litCSize = int(lhc >> 18)
		default:
			if len(src) < 5 {
				return nil, 0, DecodeError("zstd decompress: truncated literals section header")
			}
			lhSize = 5
			litSize = int((lhc >> 4) & 0x3FFFF)
			litCSize = int(lhc>>22) + int(src[4])<<10
		}
		if litSize > st.blockMax {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: %d literals exceed the block size limit %d", litSize, st.blockMax))
		}
		if !singleStream && litSize < zstdMinLitsFor4Streams {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: %d literals are not enough for the 4-stream mode (min %d)", litSize, zstdMinLitsFor4Streams))
		}
		if litCSize > len(src)-lhSize {
			return nil, 0, DecodeError("zstd decompress: truncated Huffman-compressed literals")
		}
		section := src[lhSize : lhSize+litCSize]
		literals := make([]byte, litSize)
		if blockType == zstdLitCompressed {
			t, used, err := zstdDecodeHufTable(section)
			if err != nil {
				return nil, 0, err
			}
			st.huf = t
			st.litEntropy = true
			section = section[used:]
		} else if !st.litEntropy || st.huf == nil {
			return nil, 0, DecodeError("zstd decompress: treeless literals block without a previous Huffman tree")
		}
		if litSize == 0 {
			if len(section) != 0 {
				return nil, 0, DecodeError("zstd decompress: extraneous Huffman data after empty literals")
			}
			return literals, lhSize + litCSize, nil
		}
		if singleStream {
			n, err := zstdHufDecodeStream(st.huf, section, literals)
			if err != nil {
				return nil, 0, err
			}
			if n != litSize {
				return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: Huffman stream decodes %d literals, expected %d", n, litSize))
			}
			return literals, lhSize + litCSize, nil
		}
		// 4 streams: 3 little-endian 16-bit sizes, the fourth is what remains.
		if len(section) < 6+4 {
			return nil, 0, DecodeError("zstd decompress: truncated Huffman stream sizes")
		}
		sizes := [4]int{
			int(binary.LittleEndian.Uint16(section)),
			int(binary.LittleEndian.Uint16(section[2:])),
			int(binary.LittleEndian.Uint16(section[4:])),
		}
		sizes[3] = len(section) - 6 - sizes[0] - sizes[1] - sizes[2]
		written := 0
		at := 6
		for i := 0; i < 4; i++ {
			if sizes[i] < 1 || sizes[i] > len(section)-at {
				return nil, 0, DecodeError("zstd decompress: corrupted Huffman stream sizes")
			}
			n, err := zstdHufDecodeStream(st.huf, section[at:at+sizes[i]], literals[written:])
			if err != nil {
				return nil, 0, err
			}
			written += n
			at += sizes[i]
		}
		if written != litSize {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: Huffman streams decode %d literals, expected %d", written, litSize))
		}
		return literals, lhSize + litCSize, nil
	}
	return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: impossible literals block type %d", blockType))
}

// zstdDecodeSequences decodes the Sequences_Section_Header, up to three FSE
// table descriptions and the interleaved sequence bitstream.
func zstdDecodeSequences(st *zstdBlockState, src []byte) ([]zstdSequence, error) {
	pos := 0
	if pos >= len(src) {
		return nil, DecodeError("zstd decompress: truncated sequences section header")
	}
	b0 := int(src[pos])
	pos++
	nbSeq := b0
	if b0 > 0x7F {
		if b0 == 0xFF {
			if len(src)-pos < 2 {
				return nil, DecodeError("zstd decompress: truncated sequences section header")
			}
			nbSeq = int(binary.LittleEndian.Uint16(src[pos:])) + 0x7F00
			pos += 2
		} else {
			if len(src)-pos < 1 {
				return nil, DecodeError("zstd decompress: truncated sequences section header")
			}
			nbSeq = ((b0 - 0x80) << 8) + int(src[pos])
			pos++
		}
	}
	if nbSeq == 0 {
		// "There are no sequences. The sequence section stops there."
		if pos != len(src) {
			return nil, DecodeError("zstd decompress: extraneous data present in the Sequences section")
		}
		return nil, nil
	}

	if pos >= len(src) {
		return nil, DecodeError("zstd decompress: truncated symbol compression modes")
	}
	modes := int(src[pos])
	pos++
	if modes&0x03 != 0 {
		return nil, DecodeError("zstd decompress: reserved bits set in symbol compression modes")
	}
	tables := []struct {
		mode int
		dst  **zstdFseTable
	}{
		{modes >> 6, &st.ll},       // Literals_Lengths_Mode
		{(modes >> 4) & 3, &st.of}, // Offsets_Mode
		{(modes >> 2) & 3, &st.ml}, // Match_Lengths_Mode
	}
	for i, t := range tables {
		table, used, err := zstdDecodeSeqTable(*t.dst, src[pos:], t.mode, i, st.fseEntropy)
		if err != nil {
			return nil, err
		}
		*t.dst = table
		pos += used
	}

	bitSrc := src[pos:]
	if len(bitSrc) == 0 {
		return nil, DecodeError("zstd decompress: truncated sequences bitstream")
	}
	last := int(bitSrc[len(bitSrc)-1])
	if last == 0 {
		return nil, DecodeError("zstd decompress: corrupted sequences bitstream (final bit flag missing)")
	}
	off := int64(len(bitSrc)*8) - int64(8-zstdHighBit(last))
	st.fseEntropy = true
	llState := st.ll.initBackward(bitSrc, &off)
	ofState := st.of.initBackward(bitSrc, &off)
	mlState := st.ml.initBackward(bitSrc, &off)

	seqs := make([]zstdSequence, 0, nbSeq)
	for i := 0; i < nbSeq; i++ {
		llCode := st.ll.symbols[llState]
		ofCode := st.of.symbols[ofState]
		mlCode := st.ml.symbols[mlState]
		if llCode > zstdMaxLLCode || mlCode > zstdMaxMLCode {
			return nil, DecodeError(fmt.Sprintf("zstd decompress: sequence code out of range (ll %d, ml %d)", llCode, mlCode))
		}
		// Bits are read for the offset first, then the match length, then the
		// literals length.
		off -= int64(ofCode)
		offsetValue := int64(1)<<uint(ofCode) + int64(zstdReadBitsLE(bitSrc, int(ofCode), off))
		mlBits := int(zstdMLBits[mlCode])
		off -= int64(mlBits)
		matchLength := int(zstdMLBase[mlCode]) + int(zstdReadBitsLE(bitSrc, mlBits, off))
		llBits := int(zstdLLBits[llCode])
		off -= int64(llBits)
		litLength := int(zstdLLBase[llCode]) + int(zstdReadBitsLE(bitSrc, llBits, off))
		seqs = append(seqs, zstdSequence{litLength: litLength, offsetValue: offsetValue, matchLength: matchLength})

		if i == nbSeq-1 {
			break // the last sequence does not update states
		}
		st.ll.updateBackward(&llState, bitSrc, &off)
		st.ml.updateBackward(&mlState, bitSrc, &off)
		st.of.updateBackward(&ofState, bitSrc, &off)
	}
	if off != 0 {
		return nil, DecodeError("zstd decompress: sequences bitstream not exactly consumed")
	}
	return seqs, nil
}

// zstdSeqTables describes the limits of each sequence symbol type.
var zstdSeqTables = [3]struct {
	defNorm []int16
	defSyms int
	defLog  int
	maxSyms int
	maxCode byte
	maxLog  int
}{
	{zstdLLDefaultNorm[:], zstdLLDefaultSyms, zstdLLDefaultLog, zstdLLDefaultSyms, zstdMaxLLCode, zstdSeqLLMaxLog},
	{zstdOFDefaultNorm[:], zstdOFDefaultSyms, zstdOFDefaultLog, zstdMaxOFCode + 1, zstdMaxOFCode, zstdSeqOFMaxLog},
	{zstdMLDefaultNorm[:], zstdMLDefaultSyms, zstdMLDefaultLog, zstdMLDefaultSyms, zstdMaxMLCode, zstdSeqMLMaxLog},
}

// zstdDecodeSeqTable decodes one sequence symbol table from its mode byte.
func zstdDecodeSeqTable(prev *zstdFseTable, src []byte, mode, idx int, hasPrev bool) (*zstdFseTable, int, error) {
	d := zstdSeqTables[idx]
	switch mode {
	case zstdSeqPredefined:
		table, err := zstdFseBuildTable(d.defNorm, d.defLog)
		if err != nil {
			return nil, 0, err
		}
		// Predefined tables carry no header bytes.
		return table, 0, nil
	case zstdSeqRLE:
		if len(src) < 1 {
			return nil, 0, DecodeError("zstd decompress: truncated RLE sequence table")
		}
		if src[0] > d.maxCode {
			return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: RLE symbol %d exceeds max code %d", src[0], d.maxCode))
		}
		return zstdFseRLETable(src[0]), 1, nil
	case zstdSeqFSE:
		return zstdFseDecodeTable(src, d.maxSyms, d.maxLog)
	case zstdSeqRepeat:
		if !hasPrev || prev == nil {
			return nil, 0, DecodeError("zstd decompress: Repeat_Mode sequence table without a previous table")
		}
		return prev, 0, nil
	}
	return nil, 0, DecodeError(fmt.Sprintf("zstd decompress: impossible sequence table mode %d", mode))
}

// zstdResolveOffset turns an Offset_Value into a real match offset, applying
// the repeated-offset rules and updating the offset history.
func (st *zstdBlockState) zstdResolveOffset(offsetValue int64, litLength int) (int64, error) {
	if offsetValue > 3 {
		offset := offsetValue - 3
		st.rep[2] = st.rep[1]
		st.rep[1] = st.rep[0]
		st.rep[0] = offset
		return offset, nil
	}
	idx := offsetValue - 1
	if litLength == 0 {
		idx++ // Repeated_Offset1 becomes 2, 2 becomes 3, 3 becomes 1-1byte
	}
	switch idx {
	case 0:
		return st.rep[0], nil
	case 1:
		offset := st.rep[1]
		st.rep[1] = st.rep[0]
		st.rep[0] = offset
		return offset, nil
	case 2:
		offset := st.rep[2]
		st.rep[2] = st.rep[1]
		st.rep[1] = st.rep[0]
		st.rep[0] = offset
		return offset, nil
	default:
		offset := st.rep[0] - 1
		if offset <= 0 {
			return 0, DecodeError("zstd decompress: invalid repeated offset (Repeated_Offset1 minus 1 byte)")
		}
		st.rep[2] = st.rep[1]
		st.rep[1] = st.rep[0]
		st.rep[0] = offset
		return offset, nil
	}
}

// zstdExecuteSequences interleaves literals and match copies. Match copies are
// byte by byte because a match may overlap itself, and offsets are validated
// against the whole frame output (not just the current block).
func zstdExecuteSequences(st *zstdBlockState, literals []byte, seqs []zstdSequence, out *[]byte, frameStart int, windowSize uint64) error {
	litPos := 0
	totalOutput := len(*out) - frameStart
	for _, seq := range seqs {
		if seq.litLength > len(literals)-litPos {
			return DecodeError(fmt.Sprintf("zstd decompress: sequence needs %d literals, %d left", seq.litLength, len(literals)-litPos))
		}
		*out = append(*out, literals[litPos:litPos+seq.litLength]...)
		litPos += seq.litLength
		totalOutput += seq.litLength

		offset, err := st.zstdResolveOffset(seq.offsetValue, seq.litLength)
		if err != nil {
			return err
		}
		limit := totalOutput
		if uint64(limit) > windowSize {
			limit = int(windowSize)
		}
		if offset > int64(limit) {
			return DecodeError(fmt.Sprintf("zstd decompress: match offset %d reaches before the start of the frame output (%d bytes available)", offset, limit))
		}
		start := len(*out)
		*out = append(*out, zstdRepeatByte(0, seq.matchLength)...)
		for k := 0; k < seq.matchLength; k++ {
			(*out)[start+k] = (*out)[start+k-int(offset)]
		}
		totalOutput += seq.matchLength
	}
	*out = append(*out, literals[litPos:]...)
	return nil
}

// zstdXXH64 computes the xxh64 hash of data; a zstd frame with
// Content_Checksum_flag set ends with the low 4 bytes of this hash.
func zstdXXH64(data []byte, seed uint64) uint64 {
	const (
		p1 = 0x9e3779b185ebca87
		p2 = 0xc2b2ae3d27d4eb4f
		p3 = 0x165667b19e3779f9
		p4 = 0x85ebca77c2b2ae63
		p5 = 0x27d4eb2f165667c5
	)
	round := func(acc, input uint64) uint64 {
		acc += input * p2
		acc = bits.RotateLeft64(acc, 31)
		return acc * p1
	}
	mergeRound := func(acc, val uint64) uint64 {
		acc ^= round(0, val)
		return acc*p1 + p4
	}
	n := len(data)
	pos := 0
	var h uint64
	if n >= 32 {
		v1, v2, v3, v4 := seed+p1+p2, seed+p2, seed, seed-p1
		for pos+32 <= n {
			v1 = round(v1, binary.LittleEndian.Uint64(data[pos:]))
			v2 = round(v2, binary.LittleEndian.Uint64(data[pos+8:]))
			v3 = round(v3, binary.LittleEndian.Uint64(data[pos+16:]))
			v4 = round(v4, binary.LittleEndian.Uint64(data[pos+24:]))
			pos += 32
		}
		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) + bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = mergeRound(h, v1)
		h = mergeRound(h, v2)
		h = mergeRound(h, v3)
		h = mergeRound(h, v4)
	} else {
		h = seed + p5
	}
	h += uint64(n)
	for pos+8 <= n {
		h ^= round(0, binary.LittleEndian.Uint64(data[pos:]))
		h = bits.RotateLeft64(h, 27)*p1 + p4
		pos += 8
	}
	if pos+4 <= n {
		h ^= uint64(binary.LittleEndian.Uint32(data[pos:])) * p1
		h = bits.RotateLeft64(h, 23)*p2 + p3
		pos += 4
	}
	for pos < n {
		h ^= uint64(data[pos]) * p5
		h = bits.RotateLeft64(h, 11) * p1
		pos++
	}
	h ^= h >> 33
	h *= p2
	h ^= h >> 29
	h *= p3
	h ^= h >> 32
	return h
}

func zstdRepeatByte(b byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
