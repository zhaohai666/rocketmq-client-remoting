<?php

declare(strict_types=1);

// LZ4 Frame codec + ZSTD frame codec — pure PHP core, zero third-party extensions (repo rule).
//
// LZ4: the **LZ4 Frame format** (the self-describing frame with magic 0x184D2204, which any compliant
// LZ4 decoder can decode). The internal blocks of a frame are plain LZ4 block-format — pure PHP
// encode/decode, see the block layer below.
//
// ZSTD: compression/decompression **prefers the zstd CLI** (proc_open, stdin→stdout, real compression
// ratio + able to decode standard Compressed blocks, interoperable with the real zstd on the broker
// side); when the CLI is unavailable it falls back to the pure implementation —
// compression = a legal zstd frame assembled from RAW blocks only (long runs encoded as RLE) (no
// compression ratio but wire compatible), decompression only accepts Raw/RLE blocks and raises an
// explicit error on a Compressed block instead of handing the compressed bytes back as payload
// — the wire-compatibility rule: "unsupported = throw, never passthrough".
namespace RocketMQ\Common;

final class CompressionCodec
{
    // ---------------------------------------------------------------- LZ4 block

    private const LZ4_MIN_MATCH = 4;
    // The last 5 bytes of a block must be literals (LZ4 block-format rule); the encoder stops matching
    // by the conservative rule "the last match must stay >= 12B away from the end".
    private const LZ4_MF_LIMIT = 12;
    private const LZ4_HASH_LOG = 16;
    private const LZ4_HASH_SIZE = 1 << 16;

    /** The low 32 bits of u32 * 2654435761, then shifted right by 16 bits (pure int arithmetic, no float overflow). */
    private static function lz4Hash(int $u32): int
    {
        // A direct 64-bit int multiply would exceed PHP_INT_MAX, so split into 16-bit halves to keep the low 32 bits.
        $lo = $u32 & 0xFFFF;
        $hi = $u32 >> 16;
        $kLo = 2654435761 & 0xFFFF;
        $kHi = 2654435761 >> 16;
        $low32 = ($lo * $kLo + ((($lo * $kHi) + ($hi * $kLo)) & 0xFFFF) * 65536) & 0xFFFFFFFF;
        return $low32 >> (32 - self::LZ4_HASH_LOG);
    }

    private static function readU32(string $data, int $p): int
    {
        return unpack('V', substr($data, $p, 4))[1];
    }

    /**
     * Encodes $data into one LZ4 block (no frame header; every internal block of the Frame layer is this layer).
     */
    public static function lz4CompressBlock(string $data): string
    {
        $n = strlen($data);
        if ($n === 0) {
            return '';
        }
        $out = '';
        $table = array_fill(0, self::LZ4_HASH_SIZE, -1);
        $anchor = 0;
        $i = 0;

        // emitSequence: token -> literals -> offset -> match-length extension (wire order).
        // matchLen < 4 means "literals only" (no match).
        $emitSequence = function (int $litFrom, int $litTo, int $off = 0, int $matchLen = 0) use (&$out, $data): void {
            $litLen = $litTo - $litFrom;
            $token = 0;
            $ll = $litLen;
            if ($ll >= 15) {
                $token |= 0xF0;
                $ll -= 15;
            } else {
                $token |= $ll << 4;
                $ll = -1;
            }
            $ml = $matchLen - self::LZ4_MIN_MATCH; // <0 = literals only
            if ($ml >= 0) {
                if ($ml >= 15) {
                    $token |= 0x0F;
                    $ml -= 15;
                } else {
                    $token |= $ml;
                    $ml = -1;
                }
            }
            $out .= chr($token);
            if ($ll >= 0) {
                while ($ll >= 255) {
                    $out .= chr(255);
                    $ll -= 255;
                }
                $out .= chr($ll);
            }
            if ($litLen > 0) {
                $out .= substr($data, $litFrom, $litLen);
            }
            if ($matchLen >= self::LZ4_MIN_MATCH) {
                $out .= chr($off & 0xFF) . chr(($off >> 8) & 0xFF);
                if ($ml >= 0) {
                    while ($ml >= 255) {
                        $out .= chr(255);
                        $ml -= 255;
                    }
                    $out .= chr($ml);
                }
            }
        };

        while ($i + self::LZ4_MIN_MATCH <= $n - self::LZ4_MF_LIMIT) {
            $u = self::readU32($data, $i);
            $h = self::lz4Hash($u);
            $ref = $table[$h];
            $table[$h] = $i;
            if ($ref < 0 || $ref >= $i || ($i - $ref) > 65535 || self::readU32($data, $ref) !== $u) {
                $i++;
                continue;
            }
            // Extend the match forward. The last 5 bytes of a block must stay literals (LZ4
            // block-format rule — both reference encoders and decoders depend on it), and the match
            // must not cross n-5.
            $matchLen = self::LZ4_MIN_MATCH;
            while ($i + $matchLen < $n - 5 && $data[$ref + $matchLen] === $data[$i + $matchLen]) {
                $matchLen++;
            }
            $emitSequence($anchor, $i, $i - $ref, $matchLen);
            $i += $matchLen;
            $anchor = $i;
        }
        // Trailing literals (no more matches afterwards).
        $emitSequence($anchor, $n);
        return $out;
    }

    /** Decodes one LZ4 block; bad input throws immediately. */
    public static function lz4DecompressBlock(string $data): string
    {
        $n = strlen($data);
        $out = '';
        $src = 0;

        $extLen = function (int $len) use ($data, $n, &$src): int {
            if ($len !== 15) {
                return $len;
            }
            $b = 0;
            do {
                if ($src >= $n) {
                    throw new \RuntimeException('lz4 decompress: truncated extended length');
                }
                $b = ord($data[$src++]);
                $len += $b;
            } while ($b === 255);
            return $len;
        };

        while ($src < $n) {
            $token = ord($data[$src++]);
            $litLen = $extLen($token >> 4);
            if ($src + $litLen > $n) {
                throw new \RuntimeException('lz4 decompress: literals overrun input');
            }
            if ($litLen > 0) {
                $out .= substr($data, $src, $litLen);
                $src += $litLen;
            }
            if ($src >= $n) {
                break; // the trailing segment is literals only, the block ends here
            }
            if ($src + 2 > $n) {
                throw new \RuntimeException('lz4 decompress: truncated match offset');
            }
            $off = ord($data[$src]) | (ord($data[$src + 1]) << 8);
            $src += 2;
            if ($off === 0) {
                throw new \RuntimeException('lz4 decompress: zero match offset');
            }
            $matchLen = $extLen($token & 0x0F) + self::LZ4_MIN_MATCH;
            $o = strlen($out);
            $ref = $o - $off;
            if ($ref < 0) {
                throw new \RuntimeException('lz4 decompress: match offset before start');
            }
            // Append byte by byte (the format allows overlapping matches; the ref lands inside the
            // already emitted range, so reading and appending at the same time is safe).
            // PHP string append is O(1) amortized.
            for ($k = 0; $k < $matchLen; $k++) {
                $out .= $out[$ref + $k];
            }
        }
        return $out;
    }

    // ---------------------------------------------------------------- ZSTD frame

    private const ZSTD_MAGIC = 0xFD2FB528;
    private const ZSTD_BLOCK_MAX = 128 * 1024;

    /** zstd CLI detection (cached per process). Present → the real zstd channel; absent → null (Raw/RLE fallback). */
    private static ?string $zstdCli = null;
    private static bool $zstdCliProbed = false;

    private static function zstdCliPath(): ?string
    {
        if (!self::$zstdCliProbed) {
            self::$zstdCliProbed = true;
            $out = @shell_exec('command -v zstd 2>/dev/null');
            if (is_string($out) && trim($out) !== '') {
                self::$zstdCli = trim($out);
            }
        }
        return self::$zstdCli;
    }

    /**
     * Runs a chunk of bytes through the zstd CLI (stdin→stdout, fixed arguments, path escaped with
     * escapeshellarg, nothing hits disk, no injection surface). Returns null when the CLI is missing or
     * execution fails, so the caller takes the fallback path.
     */
    private static function zstdCliRun(string $args, string $input): ?string
    {
        $cli = self::zstdCliPath();
        if ($cli === null) {
            return null;
        }
        $cmd = escapeshellarg($cli) . ' ' . $args;
        $proc = proc_open($cmd, [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes);
        if (!is_resource($proc)) {
            return null;
        }
        fwrite($pipes[0], $input);
        fclose($pipes[0]);
        $out = stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        fclose($pipes[2]); // stderr: with -q it only has content on error, so the pipe buffer never fills
        $rc = proc_close($proc);
        if ($rc !== 0 || $out === false) {
            return null;
        }
        return $out;
    }

    /**
     * Single entry point for ZSTD compression: CLI available → real zstd (compression ratio + full
     * interoperability); otherwise fall back to the pure-implementation Raw/RLE frame (no compression
     * ratio but wire compatible).
     */
    public static function zstdCompress(string $data): string
    {
        $out = self::zstdCliRun('-q -3', $data);
        return $out !== null ? $out : self::zstdCompressRaw($data);
    }

    /**
     * Single entry point for ZSTD decompression: CLI available → full-frame decompression (including
     * Compressed blocks produced by standard zstd); CLI missing → the pure implementation (Raw/RLE
     * only, Compressed blocks still throw explicitly, the compressed bytes are never handed back as
     * payload).
     */
    public static function zstdDecompress(string $data): string
    {
        $out = self::zstdCliRun('-q -d', $data);
        return $out !== null ? $out : self::zstdDecompressFrame($data);
    }

    /**
     * Assembles a minimal legal ZSTD frame: header (single-segment + 8-byte content size)
     * + RAW/RLE blocks. The broker's zstd decoder accepts it as-is; no compression ratio, only wire
     * compatibility is guaranteed.
     */
    public static function zstdCompressRaw(string $data): string
    {
        $out = pack('V', self::ZSTD_MAGIC);
        // Frame header descriptor: FCS_Field_Size=8 (flag 3) + Single_Segment=1.
        $out .= chr(0xE0 | 0x20);
        $out .= pack('P', strlen($data));
        $n = strlen($data);
        $pos = 0;
        do {
            $chunk = min(self::ZSTD_BLOCK_MAX, $n - $pos);
            $last = ($pos + $chunk >= $n) ? 1 : 0;
            if ($n > 0 && $chunk > 1 && self::isRunOfOneByte($data, $pos, $chunk)) {
                // RLE block: 1 payload byte repeated chunk times.
                // Block header: bit0=Last_Block, bits1-2=Block_Type(RLE=1), bits3-23=size.
                $sizeBits = $chunk << 3;
                $h0 = ($sizeBits & 0xFF) | 0x02 | $last;
                $out .= chr($h0) . chr(($sizeBits >> 8) & 0xFF) . chr(($sizeBits >> 16) & 0xFF);
                $out .= $data[$pos];
            } else {
                $sizeBits = $chunk << 3; // Block_Type=0（Raw）
                $h0 = ($sizeBits & 0xFF) | $last;
                $out .= chr($h0) . chr(($sizeBits >> 8) & 0xFF) . chr(($sizeBits >> 16) & 0xFF);
                if ($chunk > 0) {
                    $out .= substr($data, $pos, $chunk);
                }
            }
            $pos += $chunk;
        } while ($pos < $n);
        return $out;
    }

    private static function isRunOfOneByte(string $data, int $from, int $len): bool
    {
        $b0 = $data[$from];
        for ($i = 1; $i < $len; $i++) {
            if ($data[$from + $i] !== $b0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Decodes a ZSTD frame that contains only Raw/RLE blocks (the shape produced by our own side and by
     * raw-frame compatible modes). Compressed blocks throw immediately — the caller turns the exception
     * into a "decode failure" and never hands the compressed bytes back.
     */
    public static function zstdDecompressFrame(string $data): string
    {
        $p = 0;
        $n = strlen($data);
        if ($n < 4) {
            throw new \RuntimeException('zstd decompress: input too short');
        }
        $magic = unpack('V', substr($data, $p, 4))[1];
        if ($magic !== self::ZSTD_MAGIC) {
            throw new \RuntimeException(sprintf('zstd decompress: bad magic 0x%x', $magic));
        }
        $p += 4;
        $desc = ord($data[$p++]);
        $fcsFlag = ($desc >> 6) & 0x3;
        $singleSegment = ($desc & 0x20) !== 0;
        $dictIdFlag = $desc & 0x3;
        if (!$singleSegment) {
            $p += 1; // window descriptor (ignored)
        }
        $dictSizes = [0, 1, 2, 4];
        $p += $dictSizes[$dictIdFlag];
        $fcsSizes = $singleSegment ? [1, 2, 4, 8] : [0, 2, 4, 8];
        $p += $fcsSizes[$fcsFlag];
        $out = '';
        while ($p < $n) {
            if ($p + 3 > $n) {
                throw new \RuntimeException('zstd decompress: truncated block header');
            }
            $h = ord($data[$p]) | (ord($data[$p + 1]) << 8) | (ord($data[$p + 2]) << 16);
            $p += 3;
            $last = ($h & 1) === 1;
            $type = ($h >> 1) & 0x3;
            $size = $h >> 3;
            if ($type === 0) {
                // Raw
                if ($p + $size > $n) {
                    throw new \RuntimeException('zstd decompress: truncated raw block');
                }
                $out .= substr($data, $p, $size);
                $p += $size;
            } elseif ($type === 1) {
                // RLE
                if ($p >= $n) {
                    throw new \RuntimeException('zstd decompress: truncated rle block');
                }
                $out .= str_repeat($data[$p], $size);
                $p += 1;
            } else {
                throw new \RuntimeException('zstd decompress: unsupported block type ' . $type
                    . ' (compressed block — this codec only reads Raw/RLE frames)');
            }
            if ($last) {
                break;
            }
        }
        return $out;
    }

    // ---------------------------------------------------------------- LZ4 frame

    // ⚠ The LZ4 in the protocol is the **LZ4 Frame format** (the self-describing frame carrying
    // magic/FLG/BD/HC), not a bare block — a bare block can only be decoded by itself, so neither the
    // broker nor a standard LZ4 implementation can decode it; and once a decompression failure is
    // swallowed, what gets handed back is the compressed bytes, which means silent data loss.
    // Frame = magic + FLG + BD + [C.Size(8B)] + HC + {BlockSize(4B) + block}*
    //          + EndMark(4B) + [C.Checksum(4B)]; the block is the bare block layer above.

    private const LZ4_MAGIC = 0x184D2204;
    private const LZ4_BLOCK_SIZE = 65536; // BD=0x40 → 64KB, the conventional LZ4 frame default

    // xxhash32 (used for the HC and C.Checksum of the LZ4 frame; pure int half-split multiplication mod 2^32)
    private const XXH_P1 = 2654435761;
    private const XXH_P2 = 2246822519;
    private const XXH_P3 = 3266489917;
    private const XXH_P4 = 668265263;
    private const XXH_P5 = 374761393;

    private static function imul32(int $a, int $b): int
    {
        // (a*b) mod 2^32: PHP int is 64-bit, a direct multiply would overflow into float, so split into
        // 16-bit halves.
        $alo = $a & 0xFFFF;
        $ahi = $a >> 16;
        $blo = $b & 0xFFFF;
        $bhi = $b >> 16;
        $ll = $alo * $blo;
        $t = ($ll >> 16) + ($alo * $bhi & 0xFFFF) + ($ahi * $blo & 0xFFFF);
        return ((($t & 0xFFFF) << 16) | ($ll & 0xFFFF)) & 0xFFFFFFFF;
    }

    private static function rotl32(int $x, int $r): int
    {
        return (($x << $r) | ($x >> (32 - $r))) & 0xFFFFFFFF;
    }

    /** xxhash32 (official algorithm; the vectors 0x02CC5D05('') / 0x32D153FF('abc') are anchored in the offline tests). */
    public static function xxh32(string $data, int $seed = 0): int
    {
        $M = 0xFFFFFFFF;
        $n = strlen($data);
        $i = 0;
        if ($n >= 16) {
            $v1 = ($seed + self::XXH_P1 + self::XXH_P2) & $M;
            $v2 = ($seed + self::XXH_P2) & $M;
            $v3 = $seed & $M;
            $v4 = ($seed - self::XXH_P1) & $M;
            $limit = $n - 16;
            do {
                $v1 = self::rotl32(self::imul32(($v1 + self::imul32(self::readU32($data, $i), self::XXH_P2)) & $M, self::XXH_P1), 13);
                $v2 = self::rotl32(self::imul32(($v2 + self::imul32(self::readU32($data, $i + 4), self::XXH_P2)) & $M, self::XXH_P1), 13);
                $v3 = self::rotl32(self::imul32(($v3 + self::imul32(self::readU32($data, $i + 8), self::XXH_P2)) & $M, self::XXH_P1), 13);
                $v4 = self::rotl32(self::imul32(($v4 + self::imul32(self::readU32($data, $i + 12), self::XXH_P2)) & $M, self::XXH_P1), 13);
                $i += 16;
            } while ($i <= $limit);
            $h = (self::rotl32($v1, 1) + self::rotl32($v2, 7) + self::rotl32($v3, 12) + self::rotl32($v4, 18)) & $M;
        } else {
            $h = ($seed + self::XXH_P5) & $M;
        }
        $h = ($h + $n) & $M;
        // The trailing 4-byte loop uses P3/P4 (not P5/P1) — the official behaviour of XXH32_finalize
        while ($i + 4 <= $n) {
            $h = self::rotl32(($h + self::imul32(self::readU32($data, $i), self::XXH_P3)) & $M, 17);
            $h = self::imul32($h, self::XXH_P4);
            $i += 4;
        }
        while ($i < $n) {
            $h = ($h + self::imul32(ord($data[$i]), self::XXH_P5)) & $M;
            $h = self::imul32(self::rotl32($h, 11), self::XXH_P1);
            $i++;
        }
        $h ^= $h >> 15;
        $h = self::imul32($h, self::XXH_P2);
        $h ^= $h >> 13;
        $h = self::imul32($h, self::XXH_P3);
        $h ^= $h >> 16;
        return $h & $M;
    }

    /**
     * Compresses into an LZ4 Frame (a standard frame, decodable by any compliant decoder).
     * FLG = version01 | B.Indep | C.Size (no block checksum, no content checksum — the simplest
     * combination the frame format allows);
     * HC = the second byte of xxh32(FLG+BD+C.Size, 0); when a block does not compress, it is stored as a
     * raw block (bit31).
     */
    public static function lz4CompressFrame(string $data): string
    {
        $flg = 0x40 | 0x20 | 0x08; // version=01, B.Indep=1, C.Size=1
        $bd = 0x40;                // BlockMaxSize=64KB
        $header = chr($flg) . chr($bd) . pack('P', strlen($data));
        $out = pack('V', self::LZ4_MAGIC) . $header . chr((self::xxh32($header) >> 8) & 0xFF);
        $n = strlen($data);
        $pos = 0;
        do {
            $chunk = substr($data, $pos, self::LZ4_BLOCK_SIZE);
            $pos += strlen($chunk);
            $block = self::lz4CompressBlock($chunk);
            if ($block === '' || strlen($block) >= strlen($chunk)) {
                // Not compressible: store a raw block (bit31 set)
                $out .= pack('V', 0x80000000 | strlen($chunk)) . $chunk;
            } else {
                $out .= pack('V', strlen($block)) . $block;
            }
        } while ($pos < $n);
        $out .= pack('V', 0); // EndMark
        return $out;
    }

    /** Decodes an LZ4 Frame; bad magic / bad HC / bad blocks all throw, the compressed bytes are never handed back as payload. */
    public static function lz4DecompressFrame(string $data): string
    {
        $n = strlen($data);
        if ($n < 7) {
            throw new \RuntimeException('lz4 frame decompress: input too short');
        }
        $magic = unpack('V', substr($data, 0, 4))[1];
        if ($magic !== self::LZ4_MAGIC) {
            throw new \RuntimeException(sprintf('lz4 frame decompress: bad magic 0x%x', $magic));
        }
        $p = 4;
        $flg = ord($data[$p++]);
        if (($flg >> 6) !== 0x1) {
            throw new \RuntimeException('lz4 frame decompress: unsupported version ' . ($flg >> 6));
        }
        $blockChecksum = ($flg & 0x10) !== 0;
        $contentSizeFlag = ($flg & 0x08) !== 0;
        $contentChecksum = ($flg & 0x04) !== 0;
        $bd = ord($data[$p++]); // BlockMaxSize (the decoder works from the size in the block header, it does not need this)
        $contentSize = 0;
        if ($contentSizeFlag) {
            if ($p + 8 > $n) {
                throw new \RuntimeException('lz4 frame decompress: truncated content size');
            }
            $contentSize = unpack('P', substr($data, $p, 8))[1];
            $p += 8;
        }
        // HC = the second byte of xxh32(FLG+BD+[C.Size]) (the header checksum exists in standard frames)
        $headerLen = $p; // number of bytes already consumed by FLG+BD+C.Size
        if ($p >= $n) {
            throw new \RuntimeException('lz4 frame decompress: truncated header checksum');
        }
        $hc = ord($data[$p++]);
        if (((self::xxh32(substr($data, 4, $headerLen - 4)) >> 8) & 0xFF) !== $hc) {
            throw new \RuntimeException('lz4 frame decompress: header checksum mismatch');
        }
        $out = '';
        while (true) {
            if ($p + 4 > $n) {
                throw new \RuntimeException('lz4 frame decompress: truncated block size');
            }
            $sizeField = unpack('V', substr($data, $p, 4))[1];
            $p += 4;
            if ($sizeField === 0) {
                break; // EndMark
            }
            $isRaw = ($sizeField & 0x80000000) !== 0;
            $blockLen = $sizeField & 0x7FFFFFFF;
            if ($p + $blockLen > $n) {
                throw new \RuntimeException('lz4 frame decompress: truncated block data');
            }
            $block = substr($data, $p, $blockLen);
            $p += $blockLen;
            $out .= $isRaw ? $block : self::lz4DecompressBlock($block);
            if ($blockChecksum) {
                $p += 4; // block checksum (when B.Checksum=1)
            }
        }
        if ($contentChecksum) {
            if ($p + 4 > $n) {
                throw new \RuntimeException('lz4 frame decompress: truncated content checksum');
            }
            $expect = unpack('V', substr($data, $p, 4))[1];
            if (self::xxh32($out) !== $expect) {
                throw new \RuntimeException('lz4 frame decompress: content checksum mismatch');
            }
        }
        if ($contentSize !== 0 && strlen($out) !== $contentSize) {
            throw new \RuntimeException('lz4 frame decompress: content size mismatch');
        }
        return $out;
    }

    // ---------------------------------------------------------------- dispatch

    /**
     * Compresses according to the compression type in MessageSysFlag (sysFlag bit8-10: 1=LZ4, 2=ZSTD,
     * 3=ZLIB). ZLIB goes through gzcompress (with level, container format RFC1950, decompressed as zlib
     * on the broker side).
     */
    public static function compressFor(string $data, int $compressionType, int $level = 5): string
    {
        return match ($compressionType) {
            MessageSysFlag::ZLIB_TYPE => self::zlibCompress($data, $level),
            MessageSysFlag::LZ4_TYPE => self::lz4CompressFrame($data),
            MessageSysFlag::ZSTD_TYPE => self::zstdCompress($data),
            default => throw new \RuntimeException('unsupported compression type: ' . $compressionType),
        };
    }

    /** The inverse operation of compressFor. */
    public static function decompressFor(string $data, int $compressionType): string
    {
        return match ($compressionType) {
            // gzcompress produces a zlib container (RFC1950), gzuncompress is the only correct decoder
            // (gzinflate only reads bare deflate, gzdecode only reads gzip RFC1952).
            MessageSysFlag::ZLIB_TYPE => self::zlibDecompress($data),
            MessageSysFlag::LZ4_TYPE => self::lz4DecompressFrame($data),
            MessageSysFlag::ZSTD_TYPE => self::zstdDecompress($data),
            default => throw new \RuntimeException('unsupported compression type: ' . $compressionType),
        };
    }

    private static function zlibCompress(string $data, int $level): string
    {
        $out = @gzcompress($data, $level);
        if ($out === false) {
            throw new \RuntimeException('zlib compress failed');
        }
        return $out;
    }

    private static function zlibDecompress(string $data): string
    {
        $out = @gzuncompress($data);
        if ($out === false) {
            throw new \RuntimeException('zlib decompress failed');
        }
        return $out;
    }
}
