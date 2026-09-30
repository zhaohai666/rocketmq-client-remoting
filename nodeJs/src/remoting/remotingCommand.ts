// -*- coding: utf-8 -*-
// RemotingCommand: RocketMQ remote command (org.apache.rocketmq.remoting.protocol.RemotingCommand).
// Wire: totalLength(4) | headerLength(4, high 8 bits = serialize type) | header | body
// Header (JSON): RemotingSerializable JSON. Header (ROCKETMQ): code(2) language(1) version(2)
//   opaque(4) flag(4) remark(int+utf8) extFields(int + key(short+utf8) value(int+utf8)).
import os from 'node:os';
import { Buffer } from 'node:buffer';
import { LanguageCode, SerializeType } from './codes.ts';
import { RemotingSerializable, RocketMQSerializable } from './serialize.ts';

const SERIALIZE_TYPE_ENV = 'ROCKETMQ_SERIALIZE_TYPE';
// Java MQVersion.CURRENT_VERSION ordinal for 5.5.1. Must be > 28 (V3_0_7_SNAPSHOT)
// so the broker will callback for 307/resetOffset. Sending 0 downgrades us.
export const CURRENT_VERSION = 515;

const RPC_TYPE = 0;
const RPC_ONEWAY = 1;

let _serializeTypeConfig: number = SerializeType.JSON;
function _loadSerializeTypeConfig() {
  const v = (process.env[SERIALIZE_TYPE_ENV] || '').trim().toUpperCase();
  if (v === 'ROCKETMQ') _serializeTypeConfig = SerializeType.ROCKETMQ;
  else _serializeTypeConfig = SerializeType.JSON;
}
_loadSerializeTypeConfig();

let _requestId = 0;
function nextRequestId(): number { return _requestId++; }

export class RemotingCommand {
  code: number;
  language: number;
  version: number;
  opaque: number;
  flag: number;
  remark: string | null;
  extFields: Record<string, string>;
  customHeader: any;
  body: Buffer | null;
  serializeTypeCurrentRpc: number;

  constructor(code = 0, customHeader: any = null, remark: string | null = null,
    opaque: number | null = null, flag = 0, body: Buffer | null = null) {
    this.code = code;
    this.language = LanguageCode.NODE_JS;
    this.version = 0;
    this.opaque = opaque == null ? nextRequestId() : opaque;
    this.flag = flag;
    this.remark = remark;
    this.extFields = {};
    this.customHeader = customHeader;
    this.body = body;
    this.serializeTypeCurrentRpc = _serializeTypeConfig;
  }

  static createNewRequestId(): number { return nextRequestId(); }

  static createRequestCommand(code: number, customHeader: any = null): RemotingCommand {
    const cmd = new RemotingCommand(code, customHeader);
    _setCmdVersion(cmd);
    return cmd;
  }
  static createResponseCommandWithHeader(code: number, customHeader: any = null): RemotingCommand {
    const cmd = new RemotingCommand(code, customHeader);
    cmd.markResponseType();
    _setCmdVersion(cmd);
    return cmd;
  }
  static createResponseCommand(code = 0, remark = 'not set any response code', classHeader: any = null): RemotingCommand | null {
    const cmd = new RemotingCommand(code, null, remark);
    cmd.markResponseType();
    _setCmdVersion(cmd);
    if (classHeader != null) {
      try { cmd.customHeader = new classHeader(); } catch (e) { return null; }
    }
    return cmd;
  }
  static buildErrorResponse(code: number, remark: string): RemotingCommand | null {
    return RemotingCommand.createResponseCommand(code, remark, null);
  }
  static getProtocolType(source: number): number { return (source >> 24) & 0xff; }
  static getHeaderLength(length: number): number { return length & 0x00ffffff; }
  static markProtocolType(source: number, stype: number): number { return ((stype & 0xff) << 24) | (source & 0x00ffffff); }

  markResponseType() { this.flag |= 1 << RPC_TYPE; }
  isResponseType(): boolean { return (this.flag & (1 << RPC_TYPE)) === (1 << RPC_TYPE); }
  markOnewayRpc() { this.flag |= 1 << RPC_ONEWAY; }
  isOnewayRpc(): boolean { return (this.flag & (1 << RPC_ONEWAY)) === (1 << RPC_ONEWAY); }
  getType(): string { return this.isResponseType() ? 'RESPONSE_COMMAND' : 'REQUEST_COMMAND'; }

  makeCustomHeaderToNet() {
    if (this.customHeader != null) {
      const fn = this.customHeader.toExtFields;
      if (typeof fn === 'function') {
        const fields = fn.call(this.customHeader);
        for (const [k, v] of Object.entries(fields)) {
          if (v != null) this.extFields[k] = String(v);
        }
      }
    }
  }

  headerEncode(): Buffer {
    this.makeCustomHeaderToNet();
    if (this.serializeTypeCurrentRpc === SerializeType.ROCKETMQ) {
      return RocketMQSerializable.rocketMqProtocolEncode(this);
    }
    return RemotingSerializable.encode(this._toDict());
  }

  encode(): Buffer {
    let length = 4;
    const headerData = this.headerEncode();
    length += headerData.length;
    const body = this.body;
    if (body != null) length += body.length;
    const out = Buffer.alloc(length + 4);
    let o = 0;
    out.writeInt32BE(length, o); o += 4;
    out.writeInt32BE(RemotingCommand.markProtocolType(headerData.length, this.serializeTypeCurrentRpc), o); o += 4;
    headerData.copy(out, o); o += headerData.length;
    if (body != null) body.copy(out, o);
    return out;
  }

  encodeHeader(bodyLength = 0): Buffer {
    let length = 4;
    const headerData = this.headerEncode();
    length += headerData.length + bodyLength;
    const out = Buffer.alloc(length + 4);
    let o = 0;
    out.writeInt32BE(length, o); o += 4;
    out.writeInt32BE(RemotingCommand.markProtocolType(headerData.length, this.serializeTypeCurrentRpc), o); o += 4;
    headerData.copy(out, o);
    return out;
  }

  static decode(data: Buffer): RemotingCommand {
    let offset = 0;
    const totalLength = data.readInt32BE(offset); offset += 4;
    if (totalLength > data.length - 4) throw new Error(`decode error, bad total length: ${totalLength}`);
    const oriHeaderLen = data.readInt32BE(offset); offset += 4;
    const headerLength = RemotingCommand.getHeaderLength(oriHeaderLen);
    if (headerLength > data.length - offset) throw new Error(`decode error, bad header length: ${headerLength}`);
    const protocolType = RemotingCommand.getProtocolType(oriHeaderLen);
    const headerData = data.subarray(offset, offset + headerLength);
    offset += headerLength;
    let cmd: RemotingCommand;
    if (protocolType === SerializeType.ROCKETMQ) {
      const fields = RocketMQSerializable.rocketMqProtocolDecode(headerData);
      cmd = new RemotingCommand(fields.code, null, fields.remark, fields.opaque, fields.flag);
      cmd.language = fields.language;
      cmd.version = fields.version;
      cmd.extFields = fields.extFields || {};
    } else {
      const obj = RemotingSerializable.decode(headerData) as any;
      cmd = new RemotingCommand(obj.code, null, obj.remark, obj.opaque, obj.flag);
      let lang = obj.language;
      if (typeof lang === 'string') lang = LanguageCode.nameToCode(lang);
      cmd.language = typeof lang === 'number' ? lang : LanguageCode.NODE_JS;
      cmd.version = obj.version | 0;
      cmd.extFields = obj.extFields || {};
    }
    cmd.serializeTypeCurrentRpc = protocolType;
    const bodyLength = data.length - offset;
    cmd.body = bodyLength > 0 ? Buffer.from(data.subarray(offset)) : null;
    return cmd;
  }

  decodeCommandCustomHeader(headerCls: any, _useFastEncode = true): any {
    const h = new headerCls();
    if (typeof h.fromExtFields === 'function') {
      h.fromExtFields(this.extFields);
    } else {
      for (const [k, v] of Object.entries(this.extFields)) {
        if (h[k] !== undefined) {
          const cur = h[k];
          if (typeof cur === 'number') {
            (h as any)[k] = (cur as number) % 1 === 0 ? parseInt(v, 10) : parseFloat(v);
          } else {
            (h as any)[k] = v;
          }
        } else {
          (h as any)[k] = v;
        }
      }
    }
    this.customHeader = h;
    return h;
  }

  _toDict(): Record<string, any> {
    const d: Record<string, any> = {
      code: this.code,
      language: this.language,
      version: this.version,
      opaque: this.opaque,
      flag: this.flag,
    };
    if (this.remark != null) d['remark'] = this.remark;
    if (Object.keys(this.extFields).length) d['extFields'] = this.extFields;
    return d;
  }

  addExtField(key: string, value: string) { this.extFields[key] = value; }
  getExtField(key: string): string | undefined { return this.extFields[key]; }
  toString() {
    return `RemotingCommand [code=${this.code}, language=${this.language}, version=${this.version}, ` +
      `opaque=${this.opaque}, flag(B)=${this.flag.toString(2)}, remark=${this.remark}, extFields=${JSON.stringify(this.extFields)}, serializeType=${this.serializeTypeCurrentRpc}]`;
  }
}

function _setCmdVersion(cmd: RemotingCommand) {
  const v = process.env['rocketmq.remoting.version'];
  if (v) {
    const n = parseInt(v, 10);
    if (!Number.isNaN(n)) { cmd.version = n; return; }
  }
  cmd.version = CURRENT_VERSION;
}

export default RemotingCommand;
