// -*- coding: utf-8 -*-
// UtilAll-style helpers (org.apache.rocketmq.common.UtilAll).
import os from 'node:os';
import { Buffer } from 'node:buffer';

const CRC_TABLE = (() => {
  const table = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[n] = c;
  }
  return table;
})();

// Standard CRC32 (poly 0xEDB88320) — matches Java UtilAll.crc32 / zlib.crc32.
export function crc32(buf) {
  const data = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
  let c = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    c = CRC_TABLE[(c ^ data[i]) & 0xff] ^ (c >>> 8);
  }
  return (c ^ 0xffffffff) >>> 0;
}

// Compression "match" field: Java UtilAll.crc32 returns (int)(value & 0x7FFFFFFF),
// dropping the top bit. Cross-language, only compare each side's own match field.
export function crc32Match(buf) {
  return crc32(buf) & 0x7fffffff;
}

// Java String.hashCode (31*h + c), 32-bit signed wrap.
export function javaStringHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  }
  return h;
}

const HEX = '0123456789ABCDEF';
export function bytes2string(bs) {
  const buf = Buffer.isBuffer(bs) ? bs : Buffer.from(bs);
  let s = '';
  for (let i = 0; i < buf.length; i++) {
    s += HEX[(buf[i] >> 4) & 0x0f] + HEX[buf[i] & 0x0f];
  }
  return s;
}

export function string2bytesHex(hexString) {
  if (!hexString) return null;
  try { return Buffer.from(hexString, 'hex'); } catch (e) { return null; }
}

export function ipAndPortToBytes(ip, port, v6 = false) {
  const addr = _ipToBytes(ip, v6);
  const out = Buffer.alloc(addr.length + 4);
  addr.copy(out, 0);
  out.writeUInt32BE(port >>> 0, addr.length);
  return out;
}

function _ipToBytes(ip, v6) {
  if (v6) return Buffer.from(ip.split(':').reduce((acc, part) => {
    // simplistic ipv6 parse via dns not available; rely on caller using 16-byte repr
    return acc;
  }, Buffer.alloc(0))); // placeholder; v6 normally built from raw bytes in decoder
  const parts = ip.split('.').map(Number);
  const b = Buffer.alloc(4);
  b.writeUInt8(parts[0] | 0, 0);
  b.writeUInt8(parts[1] | 0, 1);
  b.writeUInt8(parts[2] | 0, 2);
  b.writeUInt8(parts[3] | 0, 3);
  return b;
}

export function bytesToIpAndPort(raw) {
  const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
  if (buf.length === 8) {
    const ip = `${buf[0]}.${buf[1]}.${buf[2]}.${buf[3]}`;
    const port = buf.readUInt32BE(4);
    return [ip, port];
  }
  // ipv6: 20 bytes total (16 ip + 4 port)
  const ip = buf.slice(0, 16);
  const port = buf.readUInt32BE(16);
  return [ip, port];
}

function writeLongBE(value) {
  const b = Buffer.alloc(8);
  b.writeBigInt64BE(BigInt(value), 0);
  return b;
}

export function createMessageId(addrBytes, offset) {
  return bytes2string(Buffer.concat([addrBytes, writeLongBE(offset)]));
}

export function decodeMessageId(msgId) {
  const raw = string2bytesHex(msgId);
  const ipLen = raw.length === 16 ? 4 : 16;
  const family = raw.length === 16 ? 'IPv4' : 'IPv6';
  void family;
  let ip, port, offset;
  if (ipLen === 4) {
    ip = `${raw[0]}.${raw[1]}.${raw[2]}.${raw[3]}`;
    port = raw.readUInt32BE(4);
    offset = raw.readBigInt64BE(8);
  } else {
    ip = raw.slice(0, 16);
    port = raw.readUInt32BE(16);
    offset = raw.readBigInt64BE(20);
  }
  return { ip, port, offset: Number(offset) };
}

export function currentTimeMillis() { return Date.now(); }

export function isBlank(s) { return s == null || /^\s*$/.test(s); }

export function getPid() { return process.pid; }

export function getHostIp() {
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const ni of ifaces[name] || []) {
      if (ni.family === 'IPv4' && !ni.internal) return ni.address;
    }
  }
  return '127.0.0.1';
}

export default {
  crc32, crc32Match, javaStringHash, bytes2string, string2bytesHex,
  ipAndPortToBytes, bytesToIpAndPort, createMessageId, decodeMessageId,
  currentTimeMillis, isBlank, getPid, getHostIp,
};
