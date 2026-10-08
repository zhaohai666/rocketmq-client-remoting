<?php

declare(strict_types=1);

// LZ4 Frame codec + ZSTD frame codec — 纯 PHP 主体，零第三方扩展（repo rule）。
//
// LZ4: **LZ4 Frame 格式**（Java Lz4Compressor 用 lz4-java 的 LZ4FrameOutputStream/
// LZ4FrameInputStream，Python lz4.frame 同规范互通）。Frame 内部块就是 LZ4
// block-format —— 纯 PHP 编解码，见下方 block 层。
//
// ZSTD: 压缩/解压**优先走 zstd CLI**（proc_open，stdin→stdout，真压缩率 +
// 能解 Compressed 块，与其他端真 zstd 全互通）；CLI 不可用时退回纯实现——
// 压缩 = 只用 RAW 块（长串跑 RLE）拼出的合法 zstd 帧（无压缩率但 wire 兼容），
// 解压只认 Raw/RLE 块，遇到 Compressed 块显式报错而不是把压缩字节当正文交回
// —— 跨端规则「unsupported = throw, never passthrough」。
namespace RocketMQ\Common;

final class CompressionCodec
{
    // ---------------------------------------------------------------- LZ4 block

    private const LZ4_MIN_MATCH = 4;
    // block 末 5 字节必须是字面量（LZ4 block-format 规则）；编码器按
    // 「最后一个 match 必须距末尾 ≥12B」的保守规则停止匹配。
    private const LZ4_MF_LIMIT = 12;
    private const LZ4_HASH_LOG = 16;
    private const LZ4_HASH_SIZE = 1 << 16;

    /** u32 * 2654435761 的低 32 位再右移 16 位（纯 int 运算，不吃 float 溢出）。 */
    private static function lz4Hash(int $u32): int
    {
        // 64 位 int 直接乘会超 PHP_INT_MAX，拆 16 位半字取低 32 位。
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
     * 把 $data 编成一个 LZ4 block（无帧头 —— RocketMQ wire 存裸 block，
     * 与 lz4-java 的 compress() 一致）。
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

        // emitSequence: token -> literals -> offset -> match-length 扩展（wire 顺序）。
        // matchLen < 4 表示「只有字面量」（无 match）。
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
            $ml = $matchLen - self::LZ4_MIN_MATCH; // <0 = 只有字面量
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
            // match 向前扩展。block 末 5 字节必须留成字面量（LZ4 block-format
            // 规则 —— 参考编/解码器都依赖它），match 不能越过 n-5。
            $matchLen = self::LZ4_MIN_MATCH;
            while ($i + $matchLen < $n - 5 && $data[$ref + $matchLen] === $data[$i + $matchLen]) {
                $matchLen++;
            }
            $emitSequence($anchor, $i, $i - $ref, $matchLen);
            $i += $matchLen;
            $anchor = $i;
        }
        // 收尾字面量（后面不再有 match）。
        $emitSequence($anchor, $n);
        return $out;
    }

    /** 解一个 LZ4 block；坏输入直接抛异常。 */
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
                break; // 末段只有字面量，block 到此结束
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
            // 逐字节追加（格式允许重叠 match；ref 落在已输出区间内，
            // 边读边 append 是安全的）。PHP 字符串追加 O(1) 摊销。
            for ($k = 0; $k < $matchLen; $k++) {
                $out .= $out[$ref + $k];
            }
        }
        return $out;
    }

    // ---------------------------------------------------------------- ZSTD frame

    private const ZSTD_MAGIC = 0xFD2FB528;
    private const ZSTD_BLOCK_MAX = 128 * 1024;

    /** zstd CLI 探测（进程内缓存）。有 → 真 zstd 通道；无 → null（Raw/RLE 兜底）。 */
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
     * 经 zstd CLI 处理一段字节（stdin→stdout，参数固定、路径 escapeshellarg，
     * 不落盘、无注入面）。CLI 缺失或执行失败返回 null，由调用方走兜底路径。
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
        fclose($pipes[2]); // stderr：-q 下只有出错才有内容， Pipe 缓冲不会满
        $rc = proc_close($proc);
        if ($rc !== 0 || $out === false) {
            return null;
        }
        return $out;
    }

    /**
     * ZSTD 压缩统一入口：CLI 可用 → 真 zstd（压缩率 + 全互通）；
     * 否则退回纯实现的 Raw/RLE 帧（无压缩率但 wire 兼容）。
     */
    public static function zstdCompress(string $data): string
    {
        $out = self::zstdCliRun('-q -3', $data);
        return $out !== null ? $out : self::zstdCompressRaw($data);
    }

    /**
     * ZSTD 解压统一入口：CLI 可用 → 全帧型解压（含其他端真 zstd 压出的
     * Compressed 块）；CLI 缺失 → 纯实现（只认 Raw/RLE，Compressed 块仍显式抛，
     * 绝不把压缩字节当正文交回）。
     */
    public static function zstdDecompress(string $data): string
    {
        $out = self::zstdCliRun('-q -d', $data);
        return $out !== null ? $out : self::zstdDecompressFrame($data);
    }

    /**
     * 拼一个最小合法 ZSTD 帧：header（single-segment + 8 字节 content size）
     * + RAW/RLE 块。broker 的 zstd 解码器原样接收；无压缩率，只保证 wire 兼容。
     */
    public static function zstdCompressRaw(string $data): string
    {
        $out = pack('V', self::ZSTD_MAGIC);
        // 帧头描述符：FCS_Field_Size=8（flag 3）+ Single_Segment=1。
        $out .= chr(0xE0 | 0x20);
        $out .= pack('P', strlen($data));
        $n = strlen($data);
        $pos = 0;
        do {
            $chunk = min(self::ZSTD_BLOCK_MAX, $n - $pos);
            $last = ($pos + $chunk >= $n) ? 1 : 0;
            if ($n > 0 && $chunk > 1 && self::isRunOfOneByte($data, $pos, $chunk)) {
                // RLE 块：1 个载荷字节重复 chunk 次。
                // 块头：bit0=Last_Block，bits1-2=Block_Type(RLE=1)，bits3-23=size。
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
     * 解一个只含 Raw/RLE 块的 ZSTD 帧（本端与 raw-frame 兼容模式产出的形状）。
     * Compressed 块直接抛 —— 调用方把异常变成「解码失败」，绝不交回压缩字节。
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
            $p += 1; // window descriptor（忽略）
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

    // ⚠ Java wire 的 LZ4 是 **LZ4 Frame 格式**（Lz4Compressor 用 lz4-java 的
    // LZ4FrameOutputStream/LZ4FrameInputStream；Python lz4.frame 同规范互通），
    // 不是裸 block —— 裸 block 只能自环自解，跨端/Java 全部解不开（静默丢弃）。
    // Frame = magic + FLG + BD + [C.Size(8B)] + HC + {BlockSize(4B) + block}*
    //          + EndMark(4B) + [C.Checksum(4B)]；block 就是上面的裸 block 层。

    private const LZ4_MAGIC = 0x184D2204;
    private const LZ4_BLOCK_SIZE = 65536; // BD=0x40 → 64KB，对齐 Java/Python 默认

    // xxhash32（LZ4 frame 的 HC 与 C.Checksum 用；纯 int 拆半乘法 mod 2^32）
    private const XXH_P1 = 2654435761;
    private const XXH_P2 = 2246822519;
    private const XXH_P3 = 3266489917;
    private const XXH_P4 = 668265263;
    private const XXH_P5 = 374761393;

    private static function imul32(int $a, int $b): int
    {
        // (a*b) mod 2^32：PHP int 64 位，直接乘会溢出 float，拆 16 位半字。
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

    /** xxhash32（官方算法；向量 0x02CC5D05('') / 0x32D153FF('abc') 在离线测试里锚定）。 */
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
        // 尾部 4 字节轮用 P3/P4（不是 P5/P1）—— XXH32_finalize 的官方口径
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
     * 压成 LZ4 Frame（Java LZ4FrameOutputStream / Python lz4.frame 同规范）。
     * FLG = version01 | B.Indep | C.Size（无块校验、无内容校验 —— 对齐两端默认）；
     * HC = xxh32(FLG+BD+C.Size, 0) 的第二字节；块压不动时存 raw 块（bit31）。
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
                // 压不动：存 raw 块（bit31 置位）
                $out .= pack('V', 0x80000000 | strlen($chunk)) . $chunk;
            } else {
                $out .= pack('V', strlen($block)) . $block;
            }
        } while ($pos < $n);
        $out .= pack('V', 0); // EndMark
        return $out;
    }

    /** 解 LZ4 Frame；坏 magic/坏 HC/坏块一律抛异常，绝不把压缩字节当正文交回。 */
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
        $bd = ord($data[$p++]); // BlockMaxSize（解码端按块头 size 解，不需要它）
        $contentSize = 0;
        if ($contentSizeFlag) {
            if ($p + 8 > $n) {
                throw new \RuntimeException('lz4 frame decompress: truncated content size');
            }
            $contentSize = unpack('P', substr($data, $p, 8))[1];
            $p += 8;
        }
        // HC = xxh32(FLG+BD+[C.Size]) 的第二字节（header checksum 存在于标准帧）
        $headerLen = $p; // FLG+BD+C.Size 已消耗的字节数
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
                $p += 4; // 块校验和（B.Checksum=1 时）
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
     * 按 MessageSysFlag 的压缩类型压缩（sysFlag bit8-10：1=LZ4、2=ZSTD、3=ZLIB）。
     * ZLIB 走 gzcompress（带 level，Java Deflater 同款容器格式 RFC1950）。
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

    /** compressFor 的逆运算。 */
    public static function decompressFor(string $data, int $compressionType): string
    {
        return match ($compressionType) {
            // gzcompress 产出 zlib 容器（RFC1950），gzuncompress 是唯一对的解法
            // （gzinflate 只吃裸 deflate，gzdecode 只吃 gzip RFC1952）。
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
