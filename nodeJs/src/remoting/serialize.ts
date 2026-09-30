// -*- coding: utf-8 -*-
// Protocol serialization: JSON (RemotingSerializable) + fastjson2-tolerant parser
// + RocketMQ private binary header (RocketMQSerializable).
// Mirrors python/rocketmq/remoting/protocol/serialize.py.
import { Buffer } from 'node:buffer';

// ---------------------------------------------------------------------------
// fastjson2 tolerant JSON parser
// fastjson2 output is NOT strict JSON: map numeric keys lack quotes
// ({"brokerAddrs":{0:"127.0.0.1:10911"}}), map object keys are inlined as
// JSON ({"offsetTable":{{"brokerName":..}:{...}}}), and special floats
// (NaN/Infinity) appear. We need a small parser that tolerates non-string keys.
// ---------------------------------------------------------------------------
const WS = ' \t\r\n';
const ESCAPES = { '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' };

class FastJsonParseError extends Error {}

class _FastJsonParser {
  constructor(text) { this.text = text; this.pos = 0; this.size = text.length; }

  _skipWs() { while (this.pos < this.size && WS.includes(this.text[this.pos])) this.pos++; }
  _peek() {
    if (this.pos >= this.size) throw new FastJsonParseError(`unexpected end at ${this.pos}`);
    return this.text[this.pos];
  }

  parse() {
    const value = this._value();
    this._skipWs();
    if (this.pos !== this.size) throw new FastJsonParseError(`trailing data at ${this.pos}`);
    return value;
  }

  _value() {
    this._skipWs();
    const c = this._peek();
    if (c === '{') return this._object();
    if (c === '[') return this._array();
    if (c === '"') return this._string();
    return this._literal();
  }

  _object() {
    this.pos++; // eat '{'
    const result = {};
    this._skipWs();
    if (this._peek() === '}') { this.pos++; return result; }
    for (;;) {
      const key = this._key();
      this._skipWs();
      if (this._peek() !== ':') throw new FastJsonParseError(`expected ':' at ${this.pos}`);
      this.pos++;
      result[key] = this._value();
      this._skipWs();
      const c = this._peek();
      if (c === ',') {
        this.pos++;
        this._skipWs();
        if (this._peek() === '}') { this.pos++; return result; }
        continue;
      }
      if (c === '}') { this.pos++; return result; }
      throw new FastJsonParseError(`expected ',' or '}' at ${this.pos}`);
    }
  }

  _array() {
    this.pos++; // eat '['
    const items = [];
    this._skipWs();
    if (this._peek() === ']') { this.pos++; return items; }
    for (;;) {
      items.push(this._value());
      this._skipWs();
      const c = this._peek();
      if (c === ',') {
        this.pos++;
        this._skipWs();
        if (this._peek() === ']') { this.pos++; return items; }
        continue;
      }
      if (c === ']') { this.pos++; return items; }
      throw new FastJsonParseError(`expected ',' or ']' at ${this.pos}`);
    }
  }

  _key() {
    this._skipWs();
    const c = this._peek();
    if (c === '"') return this._string();
    if (c === '{' || c === '[') {
      const start = this.pos;
      this._value();
      return this.text.slice(start, this.pos);
    }
    const start = this.pos;
    while (this.pos < this.size && !': \t\r\n'.includes(this.text[this.pos])) this.pos++;
    return this.text.slice(start, this.pos);
  }

  _string() {
    this.pos++; // eat opening quote
    let out = '';
    for (;;) {
      if (this.pos >= this.size) throw new FastJsonParseError('unterminated string');
      const c = this.text[this.pos];
      if (c === '"') { this.pos++; return out; }
      if (c === '\\') {
        this.pos++;
        if (this.pos >= this.size) throw new FastJsonParseError('unterminated escape');
        const e = this.text[this.pos];
        if (e === 'u') {
          const digits = this.text.slice(this.pos + 1, this.pos + 5);
          if (digits.length !== 4) throw new FastJsonParseError('bad \\u escape');
          out += String.fromCharCode(parseInt(digits, 16));
          this.pos += 5;
          continue;
        }
        out += (ESCAPES[e] != null ? ESCAPES[e] : e);
        this.pos++;
        continue;
      }
      out += c;
      this.pos++;
    }
  }

  _literal() {
    const rest = this.text.slice(this.pos);
    for (const [token, value] of [['true', true], ['false', false], ['null', null],
      ['NaN', NaN], ['Infinity', Infinity], ['-Infinity', -Infinity]]) {
      if (rest.startsWith(token)) {
        const tail = rest.slice(token.length, token.length + 1);
        if (tail === '' || ',}] \t\r\n'.includes(tail)) { this.pos += token.length; return value; }
      }
    }
    const m = /^-?\d+(\.\d+)?([eE][-+]?\d+)?/.exec(rest);
    if (!m) throw new FastJsonParseError(`unexpected token at ${this.pos}: ${rest.slice(0, 20)}`);
    this.pos += m[0].length;
    return m[0].includes('.') || m[0].includes('e') || m[0].includes('E') ? parseFloat(m[0]) : parseInt(m[0], 10);
  }
}

export function fastjsonLoads(text) { return new _FastJsonParser(text).parse(); }

export function decodeMessageQueueKey(key) {
  const text = (key || '').trim();
  if (!text.startsWith('{')) return null;
  try { return fastjsonLoads(text); } catch (e) { return null; }
}

function _tolerantJsonLoads(text) {
  try { return JSON.parse(text); } catch (e1) { /* fall through */ }
  try { return fastjsonLoads(text); } catch (e2) { /* fall through */ }
  const fixed = text.replace(/(?:[{,]\s*)(-?\d+(?:\.\d+)?)\s*(:)/g, (m, n, c) => `${m.slice(0, m.indexOf(n))}"${n}"${c}`);
  return JSON.parse(fixed);
}

export class RemotingSerializable {
  static encode(obj) {
    if (obj == null) return Buffer.alloc(0);
    if (Buffer.isBuffer(obj)) return obj;
    return Buffer.from(JSON.stringify(obj, jsonReplacer), 'utf-8');
  }

  static toJson(obj, pretty = false) {
    return JSON.stringify(obj, jsonReplacer, pretty ? 2 : undefined);
  }

  static decode(data) {
    if (data == null || data.length === 0) return null;
    return _tolerantJsonLoads(data.toString('utf-8'));
  }

  static fromJson(jsonStr) { return _tolerantJsonLoads(jsonStr); }

  static decodeJson(data) {
    if (data == null) return null;
    return _tolerantJsonLoads(data.toString('utf-8'));
  }
}

function jsonReplacer(_key, value) {
  if (value === undefined) return undefined;
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Set) return Array.from(value);
  if (value instanceof Map) {
    const o = {};
    for (const [k, v] of value) o[k] = v;
    return o;
  }
  return value;
}

// ---------------------------------------------------------------------------
// RocketMQ 4.x private binary header codec (RocketMQSerializable)
// Layout: code(2) language(1) version(2) opaque(4) flag(4)
//         remark(int-len utf8)  extFields(int-len map of short-key/int-value)
// ---------------------------------------------------------------------------
export class RocketMQSerializable {
  // Write a UTF-8 string into `buf` at `offset` with a length prefix
  // (short if useShortLength, else int). Returns the new offset.
  static writeStr(buf, offset, useShortLength, s) {
    const bs = (s == null) ? Buffer.alloc(0) : Buffer.from(String(s), 'utf-8');
    const n = bs.length;
    let o = offset;
    if (useShortLength) { buf.writeUInt16BE(n, o); o += 2; }
    else { buf.writeUInt32BE(n, o); o += 4; }
    bs.copy(buf, o); o += n;
    return o;
  }

  static readStr(buf, offset, useShortLength) {
    let n, o = offset;
    if (useShortLength) { n = buf.readUInt16BE(o); o += 2; }
    else { n = buf.readUInt32BE(o); o += 4; }
    if (n === 0) return [null, o];
    const s = buf.slice(o, o + n).toString('utf-8');
    return [s, o + n];
  }

  static mapSerialize(mapData) {
    if (!mapData || Object.keys(mapData).length === 0) return null;
    const entries = [];
    let total = 0;
    for (const [k, v] of Object.entries(mapData)) {
      if (k == null || v == null) continue;
      const kb = Buffer.from(String(k), 'utf-8');
      const vb = Buffer.from(String(v), 'utf-8');
      total += 2 + kb.length + 4 + vb.length;
      entries.push([kb, vb]);
    }
    const out = Buffer.alloc(total);
    let o = 0;
    for (const [kb, vb] of entries) {
      o = RocketMQSerializable.writeStr(out, o, true, kb);
      o = RocketMQSerializable.writeStr(out, o, false, vb);
    }
    return out;
  }

  static calTotalLen(remark, ext) {
    const remarkLen = remark ? Buffer.byteLength(remark, 'utf-8') : 0;
    const extLen = ext ? ext.length : 0;
    return 2 + 1 + 2 + 4 + 4 + 4 + remarkLen + 4 + extLen;
  }

  static rocketMqProtocolEncode(cmd) {
    const remark = cmd.remark;
    const remarkBytes = remark ? Buffer.from(remark, 'utf-8') : null;
    const extFieldsBytes = RocketMQSerializable.mapSerialize(cmd.extFields);
    const totalLen = RocketMQSerializable.calTotalLen(remark, extFieldsBytes);
    const buf = Buffer.alloc(totalLen);
    let o = 0;
    buf.writeInt16BE(cmd.code & 0xffff, o); o += 2;
    buf.writeUInt8(cmd.language & 0xff, o); o += 1;
    buf.writeInt16BE(cmd.version & 0xffff, o); o += 2;
    buf.writeInt32BE(cmd.opaque | 0, o); o += 4;
    buf.writeInt32BE(cmd.flag | 0, o); o += 4;
    if (remarkBytes) {
      buf.writeInt32BE(remarkBytes.length, o); o += 4;
      remarkBytes.copy(buf, o); o += remarkBytes.length;
    } else {
      buf.writeInt32BE(0, o); o += 4;
    }
    if (extFieldsBytes) {
      buf.writeInt32BE(extFieldsBytes.length, o); o += 4;
      extFieldsBytes.copy(buf, o); o += extFieldsBytes.length;
    } else {
      buf.writeInt32BE(0, o); o += 4;
    }
    return buf;
  }

  static mapDeserialize(buf, offset, length) {
    const mapData = {};
    let o = offset;
    const end = offset + length;
    while (o < end) {
      let k, v;
      [k, o] = RocketMQSerializable.readStr(buf, o, true);
      [v, o] = RocketMQSerializable.readStr(buf, o, false);
      if (k != null && v != null) mapData[k] = v;
    }
    return [mapData, o];
  }

  static rocketMqProtocolDecode(headerBytes) {
    const buf = Buffer.isBuffer(headerBytes) ? headerBytes : Buffer.from(headerBytes);
    let o = 0;
    let code = buf.readInt16BE(o); o += 2;
    let language = buf.readUInt8(o); o += 1;
    let version = buf.readInt16BE(o); o += 2;
    let opaque = buf.readInt32BE(o); o += 4;
    let flag = buf.readInt32BE(o); o += 4;
    let remark, extFields = null;
    [remark, o] = RocketMQSerializable.readStr(buf, o, false);
    const extLen = buf.readInt32BE(o); o += 4;
    if (extLen > 0) { [extFields, o] = RocketMQSerializable.mapDeserialize(buf, o, extLen); }
    return { code: code & 0xffff, language, version: version & 0xffff, opaque, flag, remark, extFields: extFields || {} };
  }
}

export default { RemotingSerializable, RocketMQSerializable, fastjsonLoads, decodeMessageQueueKey };
