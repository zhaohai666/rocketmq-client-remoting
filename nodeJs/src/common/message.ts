// -*- coding: utf-8 -*-
// Message model: MessageQueue, Message, MessageExt, MessageBatch.
// Mirrors org.apache.rocketmq.common.message.*.
import { encodeMessages } from './messageDecoder.ts';

// Java hashCode for MessageQueue: ((31 + brokerHash) * 31 + queueId) * 31 + topicHash, 32-bit signed.
function _strHash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}
function _cInt32(x: number): number { return x | 0; }

export class MessageQueue {
  topic: string;
  brokerName: string;
  queueId: number;
  constructor(topic = '', brokerName = '', queueId = 0) {
    this.topic = topic;
    this.brokerName = brokerName;
    this.queueId = queueId;
  }
  getTopic() { return this.topic; }
  setTopic(t: string) { this.topic = t; }
  getBrokerName() { return this.brokerName; }
  setBrokerName(b: string) { this.brokerName = b; }
  getQueueId() { return this.queueId; }
  getQueueIdStr() { return String(this.queueId); }
  setQueueId(q: number) { this.queueId = q; }
  hashcode(): number {
    return _cInt32(Math.imul(Math.imul(31 + _strHash(this.brokerName), 31) + this.queueId, 31) + _strHash(this.topic));
  }
  equals(other: any): boolean {
    return other instanceof MessageQueue && this.topic === other.topic &&
      this.brokerName === other.brokerName && this.queueId === other.queueId;
  }
  compareTo(other: MessageQueue): number {
    if (this.topic !== other.topic) return this.topic < other.topic ? -1 : 1;
    if (this.brokerName !== other.brokerName) return this.brokerName < other.brokerName ? -1 : 1;
    if (this.queueId !== other.queueId) return this.queueId < other.queueId ? -1 : 1;
    return 0;
  }
  toString() { return `MessageQueue [topic=${this.topic}, brokerName=${this.brokerName}, queueId=${this.queueId}]`; }
}

// Java: isWaitStoreMsgOK() — property absent => true, else "true" (case-insensitive) only.
export function isWaitStoreMsgOK(msg: any): boolean {
  const v = msg.getWaitStoreMsgOK ? msg.getWaitStoreMsgOK() : null;
  if (v == null) return true;
  return String(v).toLowerCase() === 'true';
}

export class Message {
  topic: string;
  flag: number;
  properties: Record<string, string>;
  body: Buffer;
  transactionId: string | null;
  constructor(topic = '', body: Buffer | null = null, tags: string | null = null, keys: string | null = null, flag = 0) {
    this.topic = topic;
    this.flag = flag;
    this.properties = {};
    this.body = body == null ? Buffer.alloc(0) : (Buffer.isBuffer(body) ? body : Buffer.from(body));
    this.transactionId = null;
    if (tags != null && tags !== '') this.properties['TAGS'] = tags;
    if (keys != null && keys !== '') this.properties['KEYS'] = keys;
  }
  setTags(t: string) { this.properties['TAGS'] = t; }
  getTags() { return this.properties['TAGS'] || null; }
  setKeys(k: string) { this.properties['KEYS'] = k; }
  getKeys() { return this.properties['KEYS'] || null; }
  setDelayTimeLevel(level: number) { this.properties['DELAY'] = String(level); }
  getDelayTimeLevel() { return this.properties['DELAY'] || null; }
  // Java Message#setDelayTimeSec (5.x timer wheel): delivered `sec` seconds
  // from now, expressed through the TIMER_DELAY_SEC property alias of DELAY.
  setDelayTimeSec(sec: number) { this.properties['TIMER_DELAY_SEC'] = String(sec); }
  getDelayTimeSec() { return this.properties['TIMER_DELAY_SEC'] || null; }
  // Java Message#setDelayTimeMs: delivered `ms` milliseconds from now.
  setDelayTimeMs(ms: number) { this.properties['TIMER_DELAY_MS'] = String(ms); }
  getDelayTimeMs() { return this.properties['TIMER_DELAY_MS'] || null; }
  // Java Message#setDeliverTimeMs: delivered AT the given wall-clock timestamp.
  setDeliverTimeMs(ms: number) { this.properties['TIMER_DELIVER_MS'] = String(ms); }
  getDeliverTimeMs() { return this.properties['TIMER_DELIVER_MS'] || null; }
  setWaitStoreMsgOK(ok: boolean) { this.properties['WAIT'] = ok ? 'true' : 'false'; }
  getWaitStoreMsgOK() { return this.properties['WAIT'] || null; }
  setUserProperty(name: string, value: string) { this.properties[name] = value; }
  getUserProperty(name: string) { return this.properties[name] || null; }
  putProperty(name: string, value: string) { this.properties[name] = value; }
  getProperty(name: string) { return this.properties[name] || null; }
  removeProperty(name: string) { delete this.properties[name]; }
  clearProperty() { this.properties = {}; }
  getTopic() { return this.topic; }
  setTopic(t: string) { this.topic = t; }
  getBody() { return this.body; }
  setBody(b: Buffer) { this.body = Buffer.isBuffer(b) ? b : Buffer.from(b); }
  getFlag() { return this.flag; }
  setFlag(f: number) { this.flag = f; }
  getProperties() { return this.properties; }
  setProperties(p: Record<string, string>) { this.properties = p || {}; }
  getTransactionId() { return this.transactionId; }
  setTransactionId(id: string) { this.transactionId = id; }
  toString() { return `Message(topic='${this.topic}', body=${this.body.length} bytes)`; }
}

export class MessageExt extends Message {
  queueId = 0;
  storeSize = 0;
  queueOffset = 0;
  sysFlag = 0;
  bornTimestamp = 0;
  bornHost: string | null = null;
  bornHostPort = 0;
  storeTimestamp = 0;
  storeHost: string | null = null;
  storeHostPort = 0;
  msgId: string | null = null;
  commitLogOffset = 0;
  bodyCrc = 0;
  reconsumeTimes = 0;
  preparedTransactionOffset = 0;
  brokerName: string | null = null;
  offsetMsgId: string | null = null;
  msgType: string | null = null;
  constructor(topic = '', body: Buffer | null = null, tags: string | null = null, keys: string | null = null, flag = 0) {
    super(topic, body, tags, keys, flag);
  }
  getQueueId() { return this.queueId; }
  setQueueId(q: number) { this.queueId = q; }
  getStoreSize() { return this.storeSize; }
  setStoreSize(s: number) { this.storeSize = s; }
  getQueueOffset() { return this.queueOffset; }
  setQueueOffset(o: number) { this.queueOffset = o; }
  getSysFlag() { return this.sysFlag; }
  setSysFlag(f: number) { this.sysFlag = f; }
  getBornTimestamp() { return this.bornTimestamp; }
  setBornTimestamp(t: number) { this.bornTimestamp = t; }
  getBornHost() { return this.bornHost; }
  setBornHost(h: string) { this.bornHost = h; }
  getStoreTimestamp() { return this.storeTimestamp; }
  setStoreTimestamp(t: number) { this.storeTimestamp = t; }
  getStoreHost() { return this.storeHost; }
  setStoreHost(h: string) { this.storeHost = h; }
  getMsgId() { return this.msgId; }
  setMsgId(id: string) { this.msgId = id; }
  getCommitLogOffset() { return this.commitLogOffset; }
  setCommitLogOffset(o: number) { this.commitLogOffset = o; }
  getBodyCrc() { return this.bodyCrc; }
  setBodyCrc(c: number) { this.bodyCrc = c; }
  getReconsumeTimes() { return this.reconsumeTimes; }
  setReconsumeTimes(n: number) { this.reconsumeTimes = n; }
  getPreparedTransactionOffset() { return this.preparedTransactionOffset; }
  setPreparedTransactionOffset(o: number) { this.preparedTransactionOffset = o; }
  setBrokerName(b: string) { this.brokerName = b; }
  getBrokerName() { return this.brokerName; }
  getOffsetMsgId() { return this.offsetMsgId; }
  setOffsetMsgId(id: string) { this.offsetMsgId = id; }
  getMsgType() { return this.msgType; }
  setMsgType(t: string) { this.msgType = t; }
  getBornHostString() { return (this.bornHost && this.bornHostPort) ? `${this.bornHost}:${this.bornHostPort}` : this.bornHost; }
  getStoreHostString() { return (this.storeHost && this.storeHostPort) ? `${this.storeHost}:${this.storeHostPort}` : this.storeHost; }
  toString() { return `MessageExt(topic='${this.topic}', msgId='${this.msgId}', queueOffset=${this.queueOffset}, body=${this.body.length} bytes)`; }
}

export class MessageBatch extends Message {
  messages: Message[];
  constructor(messages: Message[] | null = null) {
    super();
    this.messages = messages ? Array.from(messages) : [];
    // Java's MessageBatch constructor is private and generateFromList derives
    // the batch topic/waitStoreMsgOK from the FIRST message. A directly
    // constructed batch must do the same, or send() fails with
    // "The specified topic is blank".
    if (this.messages.length > 0) {
      const first = this.messages[0];
      this.setTopic(first.getTopic());
      this.setWaitStoreMsgOK(first.getWaitStoreMsgOK());
    }
  }
  encode(): Buffer { return encodeMessages(this.messages); }
  [Symbol.iterator]() { return this.messages[Symbol.iterator](); }
  get length() { return this.messages.length; }
  static generateFromList(messages: Message[]): MessageBatch {
    if (!messages || messages.length === 0) throw new Error('messages must not be null or empty');
    const list: Message[] = [];
    let first: Message | null = null;
    for (const m of messages) {
      const dl = m.getDelayTimeLevel();
      if (dl && parseInt(dl, 10) > 0) throw new Error('Delayed messages are not supported for batching');
      if ((m.getTopic() || '').startsWith('%RETRY%')) throw new Error('Retry Group is not supported for batching');
      if (first == null) first = m;
      else {
        if (first.getTopic() !== m.getTopic()) throw new Error('The topic of the messages in one batch should be the same');
        if (first.getWaitStoreMsgOK() !== m.getWaitStoreMsgOK()) throw new Error('The waitStoreMsgOK of the messages in one batch should be the same');
      }
      list.push(m);
    }
    const batch = new MessageBatch(list);
    batch.setTopic((first as Message).getTopic());
    batch.setWaitStoreMsgOK(isWaitStoreMsgOK(first));
    batch.setBody(encodeMessages(list));
    return batch;
  }
}

export default { MessageQueue, Message, MessageExt, MessageBatch, isWaitStoreMsgOK };
