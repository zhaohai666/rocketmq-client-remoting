package common

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

// Message body compression. ZLIB is the RFC1950 stream (Java DeflaterOutputStream,
// Python zlib.compress). LZ4/ZSTD have no stdlib implementation in Go and this
// module takes no third-party dependencies: both are reported as unsupported
// instead of silently passing the compressed bytes through — a caller that
// decodes would clear COMPRESSED_FLAG and hand out garbage as the body.
const (
	DefaultCompressLevel = 5
	ZstdDefaultLevel     = 3
)

// NormalizeCompressionType mirrors Java CompressionType.findByValue's backward
// compatibility: 1->LZ4, 2->ZSTD, and 0 (pre-type-bit clients) and 3 -> ZLIB.
// 0 must map to ZLIB or old compressed messages fail to decompress while the
// flag is cleared anyway — silent garbage.
func NormalizeCompressionType(compressionType int32) int32 {
	if compressionType == 0 {
		return ZlibType
	}
	return compressionType
}

// CompressionTypeName is the algorithm name, for logs/errors only.
func CompressionTypeName(compressionType int32) (string, bool) {
	switch NormalizeCompressionType(compressionType) {
	case Lz4Type:
		return "LZ4", true
	case ZstdType:
		return "ZSTD", true
	case ZlibType:
		return "ZLIB", true
	}
	return "", false
}

func unsupported(compressionType int32) error {
	return DecodeError(fmt.Sprintf("unsupported compression type: %d", compressionType))
}

// Compress mirrors Java Compressor#compress(byte[], level). As in Python, level
// only applies to ZLIB.
func Compress(data []byte, compressionType int32, level int32) ([]byte, error) {
	switch NormalizeCompressionType(compressionType) {
	case ZlibType:
		return ZlibCompress(data, level)
	case Lz4Type, ZstdType:
		return nil, unsupported(compressionType)
	default:
		return nil, unsupported(compressionType)
	}
}

// Decompress mirrors Java Compressor#decompress.
func Decompress(data []byte, compressionType int32) ([]byte, error) {
	switch NormalizeCompressionType(compressionType) {
	case ZlibType:
		return ZlibDecompress(data)
	case Lz4Type, ZstdType:
		return nil, unsupported(compressionType)
	default:
		return nil, unsupported(compressionType)
	}
}

// DecompressBody is the public entry used outside message decoding too (e.g.
// reply messages pushed bare over the wire).
func DecompressBody(data []byte, compressionType int32) ([]byte, error) {
	return Decompress(data, compressionType)
}

func ZlibCompress(data []byte, level int32) ([]byte, error) {
	if level < 0 || level > 9 {
		return nil, EncodeError(fmt.Sprintf("invalid zlib level: %d", level))
	}
	var buf bytes.Buffer
	zw, err := zlib.NewWriterLevel(&buf, int(level))
	if err != nil {
		return nil, EncodeError(fmt.Sprintf("zlib compress failed: %v", err))
	}
	if _, err := zw.Write(data); err != nil {
		return nil, EncodeError(fmt.Sprintf("zlib compress failed: %v", err))
	}
	if err := zw.Close(); err != nil {
		return nil, EncodeError(fmt.Sprintf("zlib compress failed: %v", err))
	}
	return buf.Bytes(), nil
}

func ZlibDecompress(data []byte) ([]byte, error) {
	zr, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, DecodeError(fmt.Sprintf("zlib decompress failed: %v", err))
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		zr.Close()
		// A truncated stream must error, never return partial data.
		return nil, DecodeError(fmt.Sprintf("zlib decompress failed: %v", err))
	}
	if err := zr.Close(); err != nil {
		return nil, DecodeError(fmt.Sprintf("zlib decompress failed: %v", err))
	}
	return out, nil
}
