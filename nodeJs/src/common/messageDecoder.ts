// -*- coding: utf-8 -*-
// Message binary codec (org.apache.rocketmq.common.message.MessageDecoder).
// 17-segment store format + 6-segment batch format, aligned with the Java/RocketMQ wire.
import zlib from 'node:zlib';
import { Buffer } from 'node:buffer';
import { Message, MessageExt, MessageBatch } from './message.ts';
import { MessageSysFlag } from './sysflag.ts';
import { crc32 } from './utilAll.ts';

const CHARSET = 'utf8';
const NAME_VALUE_SEPARATOR = 1;
const PROPERTY_SEPARATOR = 2;

const MESSAGE_MAGIC_CODE = -626843481;
const MESSAGE_MAGIC_CODE_V2 = -626843477;
const BLANK_MAGIC_CODE = -875286124;

const HEX = '0123456789ABCDEF';

function bytes2string(bs: Buffer): string {
  let s = '';
  for (let i = 0; i < bs.length; i++) s += HEX[(bs[i] >> 4) & 0x0f] + HEX[bs[i] & 0x0f];
  return s;
}
function writeLongBE(value: number | bigint): Buffer {
  const b = Buffer.alloc(8);
  b.writeBigInt64BE(BigInt(value), 0);
  return b;
}
function ipToBytes(ip: string, v6: boolean): Buffer {
  if (!v6) {
    const parts = ip.split('.').map(Number);
    const b = Buffer.alloc(4);
    b.writeUInt8(parts[0] | 0, 0); b.writeUInt8(parts[1] | 0, 1);
    b.writeUInt8(parts[2] | 0, 2); b.writeUInt8(parts[3] | 0, 3);
    return b;
  }
  // ipv6
  const segs = ip.split(':');
  const groups: number[] = new Array(8).fill(0);
  let zi = segs.indexOf('');
  if (zi >= 0) {
    const before = zi;
    const after = segs.length - 1 - zi;
    const missing = 8 - (before + after);
    segs.splice(zi, 1, ...new Array(missing).fill('0'));
  }
  segs.forEach((s, i) => { groups[i] = parseInt(s || '0', 16); });
  const b = Buffer.alloc(16);
  for (let i = 0; i < 8; i++) b.writeUInt16BE(groups[i], i * 2);
  return b;
}
function ipAndPortToBytes(ip: string, port: number, v6: boolean): Buffer {
  const addr = ipToBytes(ip, v6);
  const out = Buffer.alloc(addr.length + 4);
  addr.copy(out, 0);
  out.writeUInt32BE(port >>> 0, addr.length);
  return out;
}
function bytesToIpAndPort(raw: Buffer): [string, number] {
  if (raw.length === 8 || raw.length === 20) {
    const ip = `${raw[0]}.${raw[1]}.${raw[2]}.${raw[3]}`;
    const port = raw.readUInt32BE(4);
    return [ip, port];
  }
  // ipv6: 16 ip + 4 port
  const parts: string[] = [];
  for (let i = 0; i < 16; i += 2) parts.push(raw.readUInt16BE(i).toString(16));
  const ip = parts.join(':');
  const port = raw.readUInt32BE(16);
  return [ip, port];
}

export function createMessageId(addrBytes: Buffer, offset: number): string {
  return bytes2string(Buffer.concat([addrBytes, writeLongBE(offset)]));
}
export function decodeMessageId(msgId: string): { ip: string; port: number; offset: number } {
  const raw = Buffer.from(msgId, 'hex');
  const ipLen = raw.length === 16 ? 4 : 16;
  let ip: string, port: number, offset: number;
  if (ipLen === 4) {
    ip = `${raw[0]}.${raw[1]}.${raw[2]}.${raw[3]}`;
    port = raw.readUInt32BE(4);
    offset = Number(raw.readBigInt64BE(8));
  } else {
    const parts: string[] = [];
    for (let i = 0; i < 16; i += 2) parts.push(raw.readUInt16BE(i).toString(16));
    ip = parts.join(':');
    port = raw.readUInt32BE(16);
    offset = Number(raw.readBigInt64BE(20));
  }
  return { ip, port, offset };
}

export function messageProperties2String(properties?: Record<string, string> | null): string {
  if (properties == null) return '';
  let out = '';
  for (const [name, value] of Object.entries(properties)) {
    if (value == null) continue;
    out += `${name}${String.fromCharCode(NAME_VALUE_SEPARATOR)}${value}${String.fromCharCode(PROPERTY_SEPARATOR)}`;
  }
  return out;
}
export function string2MessageProperties(propertiesStr?: string | null): Record<string, string> {
  const result: Record<string, string> = {};
  if (!propertiesStr) return result;
  const sep = String.fromCharCode(PROPERTY_SEPARATOR);
  const kvSep = String.fromCharCode(NAME_VALUE_SEPARATOR);
  let index = 0;
  const length = propertiesStr.length;
  while (index < length) {
    let newIndex = propertiesStr.indexOf(sep, index);
    if (newIndex < 0) newIndex = length;
    if (newIndex - index >= 3) {
      const kv = propertiesStr.indexOf(kvSep, index);
      if (kv > index && kv < newIndex - 1) {
        result[propertiesStr.slice(index, kv)] = propertiesStr.slice(kv + 1, newIndex);
      }
    }
    index = newIndex + 1;
  }
  return result;
}

// ---- compression (ZLIB native; LZ4/ZSTD deliberately unsupported, per cross-port rule) ----
function normalizeCompressionType(ct: number): number {
  if (ct === 0) return MessageSysFlag.ZLIB_TYPE;
  return ct;
}
class UnsupportedCompressionError extends Error {}
function _compress(data: Buffer, compressionType: number): Buffer {
  const ct = normalizeCompressionType(compressionType);
  if (ct === MessageSysFlag.ZLIB_TYPE) return zlib.deflateSync(data);
  throw new UnsupportedCompressionError(`unsupported compression type: ${compressionType}`);
}
function _decompress(data: Buffer, compressionType: number): Buffer {
  const ct = normalizeCompressionType(compressionType);
  if (ct === MessageSysFlag.ZLIB_TYPE) return zlib.inflateSync(data);
  throw new UnsupportedCompressionError(`unsupported compression type: ${compressionType}`);
}
export function decompressBody(data: Buffer, compressionType: number): Buffer {
  return _decompress(data, compressionType);
}

function topicLengthSize(magicCode: number): number {
  return magicCode === MESSAGE_MAGIC_CODE_V2 ? 2 : 1;
}

export function encodeMessageExt(messageExt: MessageExt, needCompress = false): Buffer {
  let body = messageExt.getBody() || Buffer.alloc(0);
  if (needCompress && (messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG)) {
    const ct = MessageSysFlag.getCompressionType(messageExt.getSysFlag());
    body = _compress(body, ct);
  }
  const bodyLength = body.length;
  const topicBytes = Buffer.from(messageExt.getTopic(), CHARSET);
  const topicLen = topicBytes.length;
  const propsBytes = Buffer.from(messageProperties2String(messageExt.getProperties()), CHARSET);
  const propsLength = propsBytes.length;

  const sysFlag = messageExt.getSysFlag();
  const bornhostLen = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) ? 20 : 8;
  const storehostLen = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) ? 20 : 8;

  const computedSize = (4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + bornhostLen + storehostLen + 4 + 8
    + 4 + bodyLength + 1 + topicLen + 2 + propsLength);
  let storeSize = messageExt.getStoreSize();
  if (!(storeSize > 0)) storeSize = computedSize;
  storeSize = Math.max(storeSize, computedSize);

  const bornHost = messageExt.getBornHost() || '127.0.0.1';
  const bornPort = messageExt.bornHostPort || 0;
  const storeHost = messageExt.getStoreHost() || '127.0.0.1';
  const storePort = messageExt.storeHostPort || 0;

  const buf = Buffer.alloc(storeSize);
  let o = 0;
  buf.writeInt32BE(storeSize, o); o += 4;
  buf.writeInt32BE(MESSAGE_MAGIC_CODE, o); o += 4;
  buf.writeUInt32BE(messageExt.getBodyCrc() >>> 0, o); o += 4;
  buf.writeInt32BE(messageExt.getQueueId(), o); o += 4;
  buf.writeInt32BE(messageExt.getFlag(), o); o += 4;
  buf.writeBigInt64BE(BigInt(messageExt.getQueueOffset()), o); o += 8;
  buf.writeBigInt64BE(BigInt(messageExt.getCommitLogOffset()), o); o += 8;
  buf.writeInt32BE(sysFlag, o); o += 4;
  buf.writeBigInt64BE(BigInt(messageExt.getBornTimestamp()), o); o += 8;
  const bh = ipAndPortToBytes(bornHost, bornPort, !!(sysFlag & MessageSysFlag.BORNHOST_V6_FLAG));
  bh.copy(buf, o); o += bh.length;
  buf.writeBigInt64BE(BigInt(messageExt.getStoreTimestamp()), o); o += 8;
  const sh = ipAndPortToBytes(storeHost, storePort, !!(sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG));
  sh.copy(buf, o); o += sh.length;
  buf.writeInt32BE(messageExt.getReconsumeTimes(), o); o += 4;
  buf.writeBigInt64BE(BigInt(messageExt.getPreparedTransactionOffset()), o); o += 8;
  buf.writeInt32BE(bodyLength, o); o += 4;
  body.copy(buf, o); o += bodyLength;
  buf.writeUInt8(topicLen, o); o += 1;
  topicBytes.copy(buf, o); o += topicLen;
  buf.writeUInt16BE(propsLength, o); o += 2;
  propsBytes.copy(buf, o); o += propsLength;
  return buf;
}

export function decodeMessage(raw: Buffer, readBody = true, decompressBody = true,
  isClient = true, checkCrc = false): MessageExt | null {
  try {
    const msgExt = new MessageExt();
    let offset = 0;
    const storeSize = raw.readInt32BE(offset); offset += 4;
    const magicCode = raw.readInt32BE(offset); offset += 4;
    if (magicCode !== MESSAGE_MAGIC_CODE && magicCode !== MESSAGE_MAGIC_CODE_V2) return null;
    const useV2 = magicCode === MESSAGE_MAGIC_CODE_V2;
    const bodyCrc = raw.readUInt32BE(offset); offset += 4;
    const queueId = raw.readInt32BE(offset); offset += 4;
    const flag = raw.readInt32BE(offset); offset += 4;
    const queueOffset = Number(raw.readBigInt64BE(offset)); offset += 8;
    const physicOffset = Number(raw.readBigInt64BE(offset)); offset += 8;
    const sysFlag = raw.readInt32BE(offset); offset += 4;
    const bornTimestamp = Number(raw.readBigInt64BE(offset)); offset += 8;

    const bornhostLen = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) ? 20 : 8;
    const [bornHost, bornPort] = bytesToIpAndPort(raw.subarray(offset, offset + bornhostLen)); offset += bornhostLen;
    const storeTimestamp = Number(raw.readBigInt64BE(offset)); offset += 8;
    const storehostLen = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) ? 20 : 8;
    const [storeHost, storePort] = bytesToIpAndPort(raw.subarray(offset, offset + storehostLen)); offset += storehostLen;
    const reconsumeTimes = raw.readInt32BE(offset); offset += 4;
    const preparedTransactionOffset = Number(raw.readBigInt64BE(offset)); offset += 8;

    msgExt.setStoreSize(storeSize);
    msgExt.setBodyCrc(bodyCrc);
    msgExt.setQueueId(queueId);
    msgExt.setFlag(flag);
    msgExt.setQueueOffset(queueOffset);
    msgExt.setCommitLogOffset(physicOffset);
    msgExt.setSysFlag(sysFlag);
    msgExt.setBornTimestamp(bornTimestamp);
    msgExt.setBornHost(bornHost);
    msgExt.bornHostPort = bornPort;
    msgExt.setStoreTimestamp(storeTimestamp);
    msgExt.setStoreHost(storeHost);
    msgExt.storeHostPort = storePort;
    msgExt.setReconsumeTimes(reconsumeTimes);
    msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

    const bodyLen = raw.readInt32BE(offset); offset += 4;
    if (bodyLen > 0) {
      if (readBody) {
        let body = raw.subarray(offset, offset + bodyLen);
        if (checkCrc && crc32(body) !== bodyCrc) throw new Error('Msg crc is error');
        if (decompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG)) {
          const ct = MessageSysFlag.getCompressionType(sysFlag);
          body = _decompress(body, ct);
          msgExt.setSysFlag(MessageSysFlag.clearCompressedFlag(sysFlag));
        }
        msgExt.setBody(Buffer.from(body));
      } else {
        msgExt.setBody(null as any);
      }
      offset += bodyLen;
    } else {
      msgExt.setBody(null as any);
    }

    if (useV2) {
      const topicLen = raw.readUInt16BE(offset); offset += 2;
      msgExt.setTopic(raw.subarray(offset, offset + topicLen).toString(CHARSET)); offset += topicLen;
    } else {
      const topicLen = raw.readUInt8(offset); offset += 1;
      msgExt.setTopic(raw.subarray(offset, offset + topicLen).toString(CHARSET)); offset += topicLen;
    }

    const propsLength = raw.readUInt16BE(offset); offset += 2;
    if (propsLength > 0) {
      msgExt.setProperties(string2MessageProperties(raw.subarray(offset, offset + propsLength).toString(CHARSET)));
    }

    const storeAddrRaw = ipAndPortToBytes(storeHost, storePort, (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) !== 0);
    msgExt.setMsgId(createMessageId(storeAddrRaw, physicOffset));
    if (isClient) msgExt.setOffsetMsgId(msgExt.getMsgId() as string);
    return msgExt;
  } catch (e) {
    return null;
  }
}

export function decodeMessages(raw: Buffer, readBody = true): MessageExt[] {
  const result: MessageExt[] = [];
  let pos = 0;
  const total = raw.length;
  while (pos < total) {
    if (total - pos < 4) break;
    const storeSize = raw.readInt32BE(pos);
    if (storeSize <= 0 || storeSize > total - pos) break;
    const msg = decodeMessage(raw.subarray(pos, pos + storeSize), readBody);
    if (msg == null) break;
    result.push(msg);
    pos += storeSize;
  }
  return result;
}

// ---- 6-segment lightweight format for batch message body ----
export function encodeMessage(message: Message): Buffer {
  const body = message.getBody() || Buffer.alloc(0);
  const propsBytes = Buffer.from(messageProperties2String(message.getProperties()), CHARSET);
  const propsLength = propsBytes.length;
  const storeSize = 4 + 4 + 4 + 4 + 4 + body.length + 2 + propsLength;
  const buf = Buffer.alloc(storeSize);
  let o = 0;
  buf.writeInt32BE(storeSize, o); o += 4;
  buf.writeInt32BE(0, o); o += 4;
  buf.writeInt32BE(0, o); o += 4;
  buf.writeInt32BE(message.getFlag(), o); o += 4;
  buf.writeInt32BE(body.length, o); o += 4;
  body.copy(buf, o); o += body.length;
  buf.writeUInt16BE(propsLength, o); o += 2;
  propsBytes.copy(buf, o); o += propsLength;
  return buf;
}
export function encodeMessages(messages: Message[]): Buffer {
  const parts = messages.map(encodeMessage);
  return Buffer.concat(parts);
}
function decodeBatchMessage(raw: Buffer): Message {
  let offset = 0;
  offset += 4; // TOTALSIZE
  offset += 4; // MAGICCODE
  offset += 4; // BODYCRC
  const flag = raw.readInt32BE(offset); offset += 4;
  const bodyLen = raw.readInt32BE(offset); offset += 4;
  const body = raw.subarray(offset, offset + bodyLen); offset += bodyLen;
  const propsLen = raw.readUInt16BE(offset); offset += 2;
  const props = string2MessageProperties(raw.subarray(offset, offset + propsLen).toString(CHARSET));
  const msg = new Message();
  msg.setFlag(flag);
  msg.setBody(Buffer.from(body));
  msg.setProperties(props);
  return msg;
}
export function decodeBatchMessages(raw: Buffer): Message[] {
  const result: Message[] = [];
  let pos = 0;
  const total = raw.length;
  while (pos < total) {
    if (total - pos < 4) break;
    const storeSize = raw.readInt32BE(pos);
    if (storeSize <= 0 || storeSize > total - pos) break;
    result.push(decodeBatchMessage(raw.subarray(pos, pos + storeSize)));
    pos += storeSize;
  }
  return result;
}
export function countInnerMsgNum(raw: Buffer): number {
  let count = 0;
  let pos = 0;
  const total = raw.length;
  while (pos < total) {
    count++;
    const size = raw.readInt32BE(pos);
    if (size <= 0 || size > total - pos) break;
    pos += size;
  }
  return count;
}

export default {
  MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_V2, BLANK_MAGIC_CODE,
  createMessageId, decodeMessageId, messageProperties2String, string2MessageProperties,
  encodeMessageExt, decodeMessage, decodeMessages,
  encodeMessage, encodeMessages, decodeBatchMessage, decodeBatchMessages, countInnerMsgNum, decompressBody,
};
