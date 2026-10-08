// -*- coding: utf-8 -*-
// LZ4 block-format codec + a minimal ZSTD frame codec — pure TypeScript,
// zero third-party dependencies (repo rule).
//
// LZ4: full block-format compressor/decompressor (the format Java's lz4-java
// `safeDecompressor` reads — what the broker uses to decode a stored body).
//
// ZSTD: ENCODE = a legal zstd frame built from RAW blocks only (plus RLE for
// long runs). Any standard decoder (zstd-jni on the broker, the zstd CLI)
// accepts it; the payload is simply stored uncompressed inside the frame.
// DECODE supports Raw/RLE blocks and the common frame-header shapes; a
// Compressed block (produced by a real zstd encoder) is rejected with an
// explicit error instead of silently handing back compressed bytes — the
// cross-port "unsupported = throw, never passthrough" rule.
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

// zstdDecompressFrame decodes a ZSTD frame containing only Raw/RLE blocks
// (what this client and the raw-frame compat mode produce). A Compressed
// block throws — the caller turns that into "decode failed", never into
// handing back compressed bytes.
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

// compressFor encodes `data` for the given MessageSysFlag compression type
// (the value at bit 8-10 of sysFlag: 1=LZ4, 2=ZSTD, 3=ZLIB).
export function compressFor(data: Buffer, compressionType: number): Buffer {
  switch (compressionType) {
    case MessageSysFlag.ZLIB_TYPE: return zlib.deflateSync(data);
    case MessageSysFlag.LZ4_TYPE: return lz4CompressBlock(data);
    case MessageSysFlag.ZSTD_TYPE: return zstdCompressRaw(data);
    default:
      throw new UnsupportedCompressionError(`unsupported compression type: ${compressionType}`);
  }
}

// decompressFor is the inverse of compressFor.
export function decompressFor(data: Buffer, compressionType: number): Buffer {
  switch (compressionType) {
    case MessageSysFlag.ZLIB_TYPE: return zlib.inflateSync(data);
    case MessageSysFlag.LZ4_TYPE: return lz4DecompressBlock(data);
    case MessageSysFlag.ZSTD_TYPE: return zstdDecompressFrame(data);
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
  compressFor, decompressFor, compressionTypeByName, UnsupportedCompressionError,
};
