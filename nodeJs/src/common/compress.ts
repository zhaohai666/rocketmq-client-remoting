// -*- coding: utf-8 -*-
// LZ4 block + frame codec and a minimal ZSTD frame codec — pure TypeScript,
// zero third-party dependencies (repo rule).
//
// LZ4: the RocketMQ wire carries the LZ4 **FRAME** format (Java's Lz4Compressor
// wraps lz4-java's LZ4FrameOutputStream, Python uses lz4.frame, and the lz4 CLI
// speaks the same spec), so `lz4CompressFrame`/`lz4DecompressFrame` are what the
// compressor dispatch calls; the block layer below is what one frame block holds.
// See the ⚠ note further down for why the bare-block reading was wrong.
//
// ZSTD: real (de)compression through `node:zlib`'s zstd binding — Node's own
// stdlib, so still zero third-party dependencies. That matters because Java
// (zstd-jni) writes *compressed* blocks: a decoder that only understands
// Raw/RLE frames cannot read a message produced by a Java client.
// On runtimes without the binding (< 23.8) the hand-rolled codec below takes
// over: ENCODE = a legal zstd frame built from RAW blocks only (plus RLE for
// long runs), which any standard decoder accepts, and DECODE = Raw/RLE frames.
// A Compressed block on that fallback path is rejected with an explicit error
// instead of silently handing back compressed bytes — the cross-port
// "unsupported = throw, never passthrough" rule.
import zlib from 'node:zlib';
import { MessageSysFlag } from './sysflag.ts';

// ---------------------------------------------------------------- LZ4 block

const LZ4_MIN_MATCH = 4;
// Do not start a match within the last 5 bytes; encoder stops matching at
// len-12 (the conservative "last match must start ≥12B before the end" rule).
const LZ4_MF_LIMIT = 12;
const LZ4_HASH_LOG = 16;

function lz4Hash(u32: number): number {
  return Math.imul(u32, 2654435761) >>> (32 - LZ4_HASH_LOG);
}

// lz4CompressBlock encodes `data` as one LZ4 block (no frame header — the
// RocketMQ wire stores the raw block, same as lz4-java's compress()).
export function lz4CompressBlock(data: Buffer): Buffer {
  const n = data.length;
  if (n === 0) return Buffer.alloc(0);
  const out: number[] = [];
  const table = new Int32Array(1 << LZ4_HASH_LOG).fill(-1);
  let anchor = 0;
  let i = 0;
  const readU32 = (p: number) => data.readUInt32LE(p);

  // emitSequence writes token -> literals -> offset -> match-length extension
  // (the on-wire order). matchLen < 4 means "literals only" (no match).
  const emitSequence = (litFrom: number, litTo: number, off = 0, matchLen = 0) => {
    const litLen = litTo - litFrom;
    let token = 0;
    let ll = litLen;
    if (ll >= 15) { token |= 0xf0; ll -= 15; }
    else { token |= ll << 4; ll = -1; }
    let ml = matchLen - LZ4_MIN_MATCH; // -4-... ; <0 when literals-only
    if (ml >= 0) {
      if (ml >= 15) { token |= 0x0f; ml -= 15; }
      else { token |= ml; ml = -1; }
    }
    out.push(token);
    if (ll >= 0) {
      while (ll >= 255) { out.push(255); ll -= 255; }
      out.push(ll);
    }
    for (let p = litFrom; p < litTo; p++) out.push(data[p]);
    if (matchLen >= LZ4_MIN_MATCH) {
      out.push(off & 0xff);
      out.push((off >> 8) & 0xff);
      if (ml >= 0) {
        while (ml >= 255) { out.push(255); ml -= 255; }
        out.push(ml);
      }
    }
  };

  while (i + LZ4_MIN_MATCH <= n - LZ4_MF_LIMIT) {
    const u = readU32(i);
    const h = lz4Hash(u);
    const ref = table[h];
    table[h] = i;
    if (ref < 0 || ref >= i || (i - ref) > 65535 || readU32(ref) !== u) {
      i++;
      continue;
    }
    // Extend the match forward. The last 5 bytes of the block must remain
    // literals (LZ4 block-format rule — reference encoders/decoders rely on
    // it), so a match may not run past n-5.
    let matchLen = LZ4_MIN_MATCH;
    while (i + matchLen < n - 5 && data[ref + matchLen] === data[i + matchLen]) matchLen++;
    emitSequence(anchor, i, i - ref, matchLen);
    i += matchLen;
    anchor = i;
  }
  // Final literals (no match after them).
  emitSequence(anchor, n);
  return Buffer.from(out);
}

// lz4DecompressBlock decodes one LZ4 block. Throws on malformed input.
export function lz4DecompressBlock(data: Buffer): Buffer {
  const n = data.length;
  // Output grows as needed; LZ4 typically expands ≤4x, 64KB slack on top.
  let out = Buffer.allocUnsafe(data.length * 4 + 65536);
  let o = 0;
  let src = 0;
  const ensure = (extra: number) => {
    if (o + extra <= out.length) return;
    let cap = out.length * 2;
    while (o + extra > cap) cap *= 2;
    const grown = Buffer.allocUnsafe(cap);
    out.copy(grown, 0, 0, o);
    out = grown;
  };
  const extLen = (len: number): number => {
    if (len !== 15) return len;
    let b = 0;
    do {
      if (src >= n) throw new Error('lz4 decompress: truncated extended length');
      b = data[src++];
      len += b;
    } while (b === 255);
    return len;
  };
  while (src < n) {
    const token = data[src++];
    const litLen = extLen(token >> 4);
    if (src + litLen > n) throw new Error('lz4 decompress: literals overrun input');
    if (litLen > 0) {
      ensure(litLen);
      data.copy(out, o, src, src + litLen);
      o += litLen;
      src += litLen;
    }
    if (src >= n) break; // last-literals block ends here
    if (src + 2 > n) throw new Error('lz4 decompress: truncated match offset');
    const off = data[src] | (data[src + 1] << 8);
    src += 2;
    if (off === 0) throw new Error('lz4 decompress: zero match offset');
    const matchLen = extLen(token & 0x0f) + LZ4_MIN_MATCH;
    const ref = o - off;
    if (ref < 0) throw new Error('lz4 decompress: match offset before start');
    ensure(matchLen);
    // Byte-wise copy (the format allows overlapping matches).
    for (let i = 0; i < matchLen; i++) out[o + i] = out[ref + i];
    o += matchLen;
  }
  return out.subarray(0, o);
}

// ---------------------------------------------------------------- LZ4 frame

// ⚠ Java wire 的 LZ4 是 **LZ4 Frame 格式**（Lz4Compressor 用 lz4-java 的
// LZ4FrameOutputStream/LZ4FrameInputStream；Python lz4.frame 同规范互通）——
// 裸 block 只能自环自解，跨端/Java 全部解不开。此前的 block 直发是隐藏缺陷
//（node 不在跨端压缩矩阵里，直到 PHP 端补矩阵腿时才暴露）。
// Frame = magic + FLG + BD + [C.Size(8B)] + HC + {BlockSize(4B) + block}*
//         + EndMark(4B) + [C.Checksum(4B)]；block 就是上面的裸 block 层。

const LZ4_MAGIC = 0x184d2204;
const LZ4_FRAME_BLOCK_SIZE = 65536; // BD=0x40 → 64KB，对齐 Java/Python 默认

// xxhash32（Frame 的 HC 与 C.Checksum 用）。Math.imul 天然 mod 2^32。
export function xxh32(data: Buffer, seed = 0): number {
  const P1 = 2654435761, P2 = 2246822519, P3 = 3266489917, P4 = 668265263, P5 = 374761393;
  const n = data.length;
  let i = 0;
  let h: number;
  if (n >= 16) {
    let v1 = (seed + P1 + P2) >>> 0;
    let v2 = (seed + P2) >>> 0;
    let v3 = seed >>> 0;
    let v4 = (seed - P1) >>> 0;
    const limit = n - 16;
    do {
      v1 = Math.imul(Math.imul(data.readUInt32LE(i), P2) + v1, P1);
      v1 = ((v1 << 13) | (v1 >>> 19)) >>> 0;
      v2 = Math.imul(Math.imul(data.readUInt32LE(i + 4), P2) + v2, P1);
      v2 = ((v2 << 13) | (v2 >>> 19)) >>> 0;
      v3 = Math.imul(Math.imul(data.readUInt32LE(i + 8), P2) + v3, P1);
      v3 = ((v3 << 13) | (v3 >>> 19)) >>> 0;
      v4 = Math.imul(Math.imul(data.readUInt32LE(i + 12), P2) + v4, P1);
      v4 = ((v4 << 13) | (v4 >>> 19)) >>> 0;
      i += 16;
    } while (i <= limit);
    h = (((v1 << 1) | (v1 >>> 31)) + ((v2 << 7) | (v2 >>> 25))
      + ((v3 << 12) | (v3 >>> 20)) + ((v4 << 18) | (v4 >>> 14))) >>> 0;
  } else {
    h = (seed + P5) >>> 0;
  }
  h = (h + n) >>> 0;
  // 尾部 4 字节轮用 P3/P4（XXH32_finalize 官方口径；不是 P5/P1）
  while (i + 4 <= n) {
    h = (h + Math.imul(data.readUInt32LE(i), P3)) >>> 0;
    h = (((h << 17) | (h >>> 15)) >>> 0);
    h = Math.imul(h, P4);
    i += 4;
  }
  while (i < n) {
    h = (h + Math.imul(data[i], P5)) >>> 0;
    h = (((h << 11) | (h >>> 21)) >>> 0);
    h = Math.imul(h, P1);
    i++;
  }
  h ^= h >>> 15;
  h = Math.imul(h, P2);
  h ^= h >>> 13;
  h = Math.imul(h, P3);
  h ^= h >>> 16;
  return h >>> 0;
}

// lz4CompressFrame 压成 LZ4 Frame（Java LZ4FrameOutputStream / Python
// lz4.frame 同规范）。FLG = version01 | B.Indep | C.Size；无块/内容校验
//（对齐两端默认）；块压不动时存 raw 块（bit31 置位）。
export function lz4CompressFrame(data: Buffer): Buffer {
  const flg = 0x40 | 0x20 | 0x08; // version=01, B.Indep=1, C.Size=1
  const bd = 0x40; // BlockMaxSize=64KB
  const header = Buffer.from([flg, bd]);
  const cs = Buffer.alloc(8);
  cs.writeBigUInt64LE(BigInt(data.length), 0);
  const parts: Buffer[] = [Buffer.from([0x04, 0x22, 0x4d, 0x18]), header, cs];
  // HC = xxh32(FLG+BD+C.Size, 0) 的第二字节
  parts.push(Buffer.from([((xxh32(Buffer.concat([header, cs])) >>> 8) & 0xff)]));
  const n = data.length;
  let pos = 0;
  do {
    const chunk = data.subarray(pos, pos + LZ4_FRAME_BLOCK_SIZE);
    pos += chunk.length;
    const block = lz4CompressBlock(chunk);
    const sizeField = Buffer.alloc(4);
    if (block.length === 0 || block.length >= chunk.length) {
      // 压不动：存 raw 块（bit31 置位）
      sizeField.writeUInt32LE((0x80000000 | chunk.length) >>> 0, 0);
      parts.push(sizeField, chunk);
    } else {
      sizeField.writeUInt32LE(block.length, 0);
      parts.push(sizeField, block);
    }
  } while (pos < n);
  parts.push(Buffer.from([0, 0, 0, 0])); // EndMark
  return Buffer.concat(parts);
}

// lz4DecompressFrame 解 LZ4 Frame；坏 magic/坏 HC/坏块一律抛异常。
export function lz4DecompressFrame(data: Buffer): Buffer {
  if (data.length < 7) throw new Error('lz4 frame decompress: input too short');
  if (data.readUInt32LE(0) !== LZ4_MAGIC) {
    throw new Error(`lz4 frame decompress: bad magic 0x${data.readUInt32LE(0).toString(16)}`);
  }
  let p = 4;
  const flg = data[p++];
  if (flg >>> 6 !== 0x1) throw new Error(`lz4 frame decompress: unsupported version ${flg >>> 6}`);
  const blockChecksum = (flg & 0x10) !== 0;
  const contentSizeFlag = (flg & 0x08) !== 0;
  const contentChecksum = (flg & 0x04) !== 0;
  p += 1; // BD（解码按块头 size 走，BlockMaxSize 不需要）
  let contentSize = 0;
  if (contentSizeFlag) {
    if (p + 8 > data.length) throw new Error('lz4 frame decompress: truncated content size');
    contentSize = Number(data.readBigUInt64LE(p));
    p += 8;
  }
  // HC = xxh32(FLG+BD+[C.Size]) 的第二字节（header checksum 存在于标准帧）
  const headerEnd = p;
  if (p >= data.length) throw new Error('lz4 frame decompress: truncated header checksum');
  const hc = data[p++];
  if ((((xxh32(data.subarray(4, headerEnd)) >>> 8) & 0xff)) !== hc) {
    throw new Error('lz4 frame decompress: header checksum mismatch');
  }
  const parts: Buffer[] = [];
  for (;;) {
    if (p + 4 > data.length) throw new Error('lz4 frame decompress: truncated block size');
    const sizeField = data.readUInt32LE(p);
    p += 4;
    if (sizeField === 0) break; // EndMark
    const isRaw = (sizeField & 0x80000000) !== 0;
    const blockLen = sizeField & 0x7fffffff;
    if (p + blockLen > data.length) throw new Error('lz4 frame decompress: truncated block data');
    const block = data.subarray(p, p + blockLen);
    p += blockLen;
    parts.push(isRaw ? block : lz4DecompressBlock(block));
    if (blockChecksum) p += 4;
  }
  const out = Buffer.concat(parts);
  if (contentChecksum) {
    if (p + 4 > data.length) throw new Error('lz4 frame decompress: truncated content checksum');
    if (xxh32(out) !== data.readUInt32LE(p)) {
      throw new Error('lz4 frame decompress: content checksum mismatch');
    }
  }
  if (contentSize !== 0 && out.length !== contentSize) {
    throw new Error('lz4 frame decompress: content size mismatch');
  }
  return out;
}

// ---------------------------------------------------------------- ZSTD frame

const ZSTD_MAGIC = 0xfd2fb528;
const ZSTD_BLOCK_MAX = 128 * 1024;

// zstdCompressRaw builds a minimal valid ZSTD frame: header (single-segment,
// 8-byte content size) + RAW/RLE blocks. The broker's zstd decoder accepts it
// as-is; there is no compression gain, only wire compatibility.
export function zstdCompressRaw(data: Buffer): Buffer {
  const parts: Buffer[] = [];
  const magic = Buffer.alloc(4);
  magic.writeUInt32LE(ZSTD_MAGIC, 0);
  parts.push(magic);
  // Frame header descriptor: FCS_Field_Size=8 (flag 3), Single_Segment=1.
  const desc = Buffer.from([0xe0 | 0x20]);
  parts.push(desc);
  const fcs = Buffer.alloc(8);
  fcs.writeBigUInt64LE(BigInt(data.length), 0);
  parts.push(fcs);
  let pos = 0;
  while (pos < data.length || data.length === 0) {
    const chunk = Math.min(ZSTD_BLOCK_MAX, data.length - pos);
    if (data.length > 0 && chunk > 1 && isRunOfOneByte(data, pos, chunk)) {
      // RLE block: 1 payload byte repeated `chunk` times.
      // Block header: bit0=Last_Block, bits1-2=Block_Type(RLE=1), bits3-23=size.
      const sizeBits = chunk << 3;
      const header = Buffer.alloc(3);
      header[0] = (sizeBits & 0xff) | 0x02; // Block_Type = RLE
      header[1] = (sizeBits >> 8) & 0xff;
      header[2] = (sizeBits >> 16) & 0xff;
      if (pos + chunk >= data.length) header[0] |= 0x01; // last block
      parts.push(header, Buffer.from([data[pos]]));
    } else {
      const header = Buffer.alloc(3);
      const sizeBits = chunk << 3; // Block_Type=0 (Raw)
      header[0] = sizeBits & 0xff;
      header[1] = (sizeBits >> 8) & 0xff;
      header[2] = (sizeBits >> 16) & 0xff;
      if (pos + chunk >= data.length) header[0] |= 0x01; // last block
      parts.push(header, data.subarray(pos, pos + chunk));
    }
    pos += chunk;
    if (data.length === 0) break;
  }
  return Buffer.concat(parts);
}

function isRunOfOneByte(data: Buffer, from: number, len: number): boolean {
  const b0 = data[from];
  for (let i = 1; i < len; i++) {
    if (data[from + i] !== b0) return false;
  }
  return true;
}

// zstdDecompressFrame is the dependency-free fallback: it decodes a ZSTD frame
// containing only Raw/RLE blocks (what zstdCompressRaw produces, i.e. what
// older Node runtimes write themselves). A Compressed block — what a real
// encoder such as zstd-jni writes — throws; use zstdDecompress for that.
export function zstdDecompressFrame(data: Buffer): Buffer {
  let p = 0;
  if (data.length < 4) throw new Error('zstd decompress: input too short');
  const magic = data.readUInt32LE(p);
  if (magic !== ZSTD_MAGIC) throw new Error(`zstd decompress: bad magic 0x${magic.toString(16)}`);
  p += 4;
  const desc = data[p++];
  const fcsFlag = (desc >> 6) & 0x3;
  const singleSegment = (desc & 0x20) !== 0;
  const checksumFlag = (desc & 0x04) !== 0;
  const dictIdFlag = desc & 0x3;
  if (!singleSegment) p += 1; // window descriptor (ignored)
  const dictSizes = [0, 1, 2, 4];
  p += dictSizes[dictIdFlag];
  const fcsSizes = singleSegment ? [1, 2, 4, 8] : [0, 2, 4, 8];
  p += fcsSizes[fcsFlag];
  const parts: Buffer[] = [];
  while (p < data.length) {
    if (p + 3 > data.length) throw new Error('zstd decompress: truncated block header');
    const h = data[p] | (data[p + 1] << 8) | (data[p + 2] << 16);
    p += 3;
    const last = (h & 1) === 1;
    const type = (h >> 1) & 0x3;
    const size = h >> 3;
    if (type === 0) {
      // Raw
      if (p + size > data.length) throw new Error('zstd decompress: truncated raw block');
      parts.push(data.subarray(p, p + size));
      p += size;
    } else if (type === 1) {
      // RLE
      if (p >= data.length) throw new Error('zstd decompress: truncated rle block');
      parts.push(Buffer.alloc(size, data[p]));
      p += 1;
    } else {
      throw new Error(`zstd decompress: unsupported block type ${type} (compressed block — this codec only reads Raw/RLE frames)`);
    }
    if (last) break;
  }
  void checksumFlag;
  return Buffer.concat(parts);
}

// ---------------------------------------------------------------- dispatch

class UnsupportedCompressionError extends Error {}
export { UnsupportedCompressionError };

// node:zlib gained zstd bindings in Node 23.8. Feature-detected instead of
// version-checked: the fallback path is a working codec, not a crash.
const nodeZstd = (zlib as unknown as {
  zstdCompressSync?: (data: Buffer, options?: { level?: number }) => Buffer;
  zstdDecompressSync?: (data: Buffer) => Buffer;
});

const HAS_NODE_ZSTD = typeof nodeZstd.zstdCompressSync === 'function'
  && typeof nodeZstd.zstdDecompressSync === 'function';

// zstdCompress writes a real zstd frame (level 1..22; <=0 means the library
// default, which is what Java's zstd-jni uses). Without the binding it degrades
// to the store-only Raw/RLE frame — wire-legal, just no compression gain.
export function zstdCompress(data: Buffer, level?: number): Buffer {
  if (!HAS_NODE_ZSTD) return zstdCompressRaw(data);
  return level && level >= 1
    ? nodeZstd.zstdCompressSync!(data, { level })
    : nodeZstd.zstdCompressSync!(data);
}

// zstdDecompress reads any standard frame, Compressed blocks included.
export function zstdDecompress(data: Buffer): Buffer {
  if (!HAS_NODE_ZSTD) return zstdDecompressFrame(data);
  try {
    return nodeZstd.zstdDecompressSync!(data);
  } catch (e) {
    throw new Error(`zstd decompress: ${(e as Error).message}`);
  }
}

// compressFor encodes `data` for the given MessageSysFlag compression type
// (the value at bit 8-10 of sysFlag: 1=LZ4, 2=ZSTD, 3=ZLIB).
//
// `level` is the producer's compressLevel (Java DefaultMQProducer, default 5,
// range 0-9). ZLIB uses it as the deflate level; ZSTD maps it onto the zstd
// level (1-9 is legal in both scales). LZ4's block format has no level.
export function compressFor(data: Buffer, compressionType: number, level: number = 5): Buffer {
  switch (compressionType) {
    case MessageSysFlag.ZLIB_TYPE: {
      // zlib accepts 0(-none)..9; anything outside maps to the library default.
      const lv = Number.isFinite(level) && level >= 0 && level <= 9 ? Math.floor(level) : undefined;
      return lv === undefined ? zlib.deflateSync(data) : zlib.deflateSync(data, { level: lv });
    }
    case MessageSysFlag.LZ4_TYPE: return lz4CompressFrame(data);
    case MessageSysFlag.ZSTD_TYPE: return zstdCompress(data, level);
    default:
      throw new UnsupportedCompressionError(`unsupported compression type: ${compressionType}`);
  }
}

// decompressFor is the inverse of compressFor.
export function decompressFor(data: Buffer, compressionType: number): Buffer {
  switch (compressionType) {
    case MessageSysFlag.ZLIB_TYPE: return zlib.inflateSync(data);
    case MessageSysFlag.LZ4_TYPE: return lz4DecompressFrame(data);
    case MessageSysFlag.ZSTD_TYPE: return zstdDecompress(data);
    default:
      throw new UnsupportedCompressionError(`unsupported compression type: ${compressionType}`);
  }
}

// compressionTypeByName maps the producer-facing names (Java CompressionType)
// onto the sysFlag values. Accepts case-insensitive names or numeric strings.
export function compressionTypeByName(name: string | number): number {
  if (typeof name === 'number') {
    if (name === MessageSysFlag.LZ4_TYPE || name === MessageSysFlag.ZSTD_TYPE
      || name === MessageSysFlag.ZLIB_TYPE) return name;
    throw new UnsupportedCompressionError(`unsupported compression type: ${name}`);
  }
  switch (String(name).toUpperCase()) {
    case 'ZLIB': case 'DEFLATE': return MessageSysFlag.ZLIB_TYPE;
    case 'LZ4': return MessageSysFlag.LZ4_TYPE;
    case 'ZSTD': case 'ZSTANDARD': return MessageSysFlag.ZSTD_TYPE;
    default:
      throw new UnsupportedCompressionError(`unsupported compression type: ${name}`);
  }
}

export default {
  lz4CompressBlock, lz4DecompressBlock, zstdCompressRaw, zstdDecompressFrame,
  zstdCompress, zstdDecompress,
  compressFor, decompressFor, compressionTypeByName, UnsupportedCompressionError,
};
