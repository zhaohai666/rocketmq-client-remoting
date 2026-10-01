// -*- coding: utf-8 -*-
// Request/response headers (org.apache.rocketmq.remoting.protocol.header.*).
// Mirrors python/rocketmq/remoting/protocol/headers.py.
//
// Each header implements toExtFields() (return a plain object of string-coercible
// fields; null is skipped) and fromExtFields(fields) (read from a
// Record<string,string>). Booleans are normalised to Java's lowercase
// "true"/"false" so on-wire packets match the Java client byte-for-byte.
// Field names are identical to the Java wire keys.

function _ext(fields: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === null || v === undefined) continue;
    out[k] = (typeof v === 'boolean') ? (v ? 'true' : 'false') : v;
  }
  return out;
}

function _num(v: any): number | null {
  if (v == null) return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : n;
}

function _b(v: any): boolean | null {
  if (v == null) return null;
  const s = String(v).toLowerCase();
  return s === 'true' || s === '1';
}

// BoundaryType (org.apache.rocketmq.common.BoundaryType). Wire value is the
// UPPER-CASE enum name; the lowercase name is only used for parsing tolerance.
export class BoundaryType {
  static LOWER = { value: 'LOWER', lowercaseName: 'lower' };
  static UPPER = { value: 'UPPER', lowercaseName: 'upper' };
  static get_type(name: any): any {
    if (typeof name === 'string' && name.toLowerCase() === 'upper') return BoundaryType.UPPER;
    return BoundaryType.LOWER;
  }
}

export class CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  checkFields(): void { /* no-op */ }
}

export class SendMessageRequestHeader extends CommandCustomHeader {
  producerGroup: string | null = null;
  topic: string | null = null;
  defaultTopic: string | null = null;
  defaultTopicQueueNums: number | null = null;
  queueId: number | null = null;
  sysFlag: number | null = null;
  bornTimestamp: number | null = null;
  flag: number | null = null;
  properties: string | null = null;
  reconsumeTimes: number | null = null;
  unitMode: boolean | null = null;
  maxReconsumeTimes: number | null = null;
  batch: boolean | null = null;
  brokerName: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      producerGroup: this.producerGroup,
      topic: this.topic,
      defaultTopic: this.defaultTopic,
      defaultTopicQueueNums: this.defaultTopicQueueNums,
      queueId: this.queueId,
      sysFlag: this.sysFlag,
      bornTimestamp: this.bornTimestamp,
      flag: this.flag,
      properties: this.properties,
      reconsumeTimes: this.reconsumeTimes,
      unitMode: this.unitMode,
      maxReconsumeTimes: this.maxReconsumeTimes,
      batch: this.batch,
      brokerName: this.brokerName,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.producerGroup = ext['producerGroup'] != null ? ext['producerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.defaultTopic = ext['defaultTopic'] != null ? ext['defaultTopic'] : null;
    this.defaultTopicQueueNums = _num(ext['defaultTopicQueueNums']);
    this.queueId = _num(ext['queueId']);
    this.sysFlag = _num(ext['sysFlag']);
    this.bornTimestamp = _num(ext['bornTimestamp']);
    this.flag = _num(ext['flag']);
    this.properties = ext['properties'] != null ? ext['properties'] : null;
    this.reconsumeTimes = _num(ext['reconsumeTimes']);
    this.unitMode = _b(ext['unitMode']);
    this.maxReconsumeTimes = _num(ext['maxReconsumeTimes']);
    this.batch = _b(ext['batch']);
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
  }
}

// Short field-name V2 encoding (producerGroup->a ... brokerName->n).
// NOTE: Java SendMessageRequestHeaderV2 has 14 short keys (a..n), not 17.
export class SendMessageRequestHeaderV2 extends CommandCustomHeader {
  producerGroup: string | null = null;
  topic: string | null = null;
  defaultTopic: string | null = null;
  defaultTopicQueueNums: number | null = null;
  queueId: number | null = null;
  sysFlag: number | null = null;
  bornTimestamp: number | null = null;
  flag: number | null = null;
  properties: string | null = null;
  reconsumeTimes: number | null = null;
  unitMode: boolean | null = null;
  maxReconsumeTimes: number | null = null;
  batch: boolean | null = null;
  brokerName: string | null = null;

  static createV2(req: SendMessageRequestHeader): SendMessageRequestHeaderV2 {
    const v2 = new SendMessageRequestHeaderV2();
    v2.producerGroup = req.producerGroup;
    v2.topic = req.topic;
    v2.defaultTopic = req.defaultTopic;
    v2.defaultTopicQueueNums = req.defaultTopicQueueNums;
    v2.queueId = req.queueId;
    v2.sysFlag = req.sysFlag;
    v2.bornTimestamp = req.bornTimestamp;
    v2.flag = req.flag;
    v2.properties = req.properties;
    v2.reconsumeTimes = req.reconsumeTimes;
    v2.unitMode = req.unitMode;
    v2.maxReconsumeTimes = req.maxReconsumeTimes;
    v2.batch = req.batch;
    v2.brokerName = req.brokerName;
    return v2;
  }
  toV1(): SendMessageRequestHeader {
    const v1 = new SendMessageRequestHeader();
    v1.producerGroup = this.producerGroup;
    v1.topic = this.topic;
    v1.defaultTopic = this.defaultTopic;
    v1.defaultTopicQueueNums = this.defaultTopicQueueNums;
    v1.queueId = this.queueId;
    v1.sysFlag = this.sysFlag;
    v1.bornTimestamp = this.bornTimestamp;
    v1.flag = this.flag;
    v1.properties = this.properties;
    v1.reconsumeTimes = this.reconsumeTimes;
    v1.unitMode = this.unitMode;
    v1.maxReconsumeTimes = this.maxReconsumeTimes;
    v1.batch = this.batch;
    v1.brokerName = this.brokerName;
    return v1;
  }
  toExtFields(): Record<string, any> {
    return _ext({
      a: this.producerGroup,
      b: this.topic,
      c: this.defaultTopic,
      d: this.defaultTopicQueueNums,
      e: this.queueId,
      f: this.sysFlag,
      g: this.bornTimestamp,
      h: this.flag,
      i: this.properties,
      j: this.reconsumeTimes,
      k: this.unitMode,
      l: this.maxReconsumeTimes,
      m: this.batch,
      n: this.brokerName,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.producerGroup = ext['a'] != null ? ext['a'] : null;
    this.topic = ext['b'] != null ? ext['b'] : null;
    this.defaultTopic = ext['c'] != null ? ext['c'] : null;
    this.defaultTopicQueueNums = _num(ext['d']);
    this.queueId = _num(ext['e']);
    this.sysFlag = _num(ext['f']);
    this.bornTimestamp = _num(ext['g']);
    this.flag = _num(ext['h']);
    this.properties = ext['i'] != null ? ext['i'] : null;
    this.reconsumeTimes = _num(ext['j']);
    this.unitMode = _b(ext['k']);
    this.maxReconsumeTimes = _num(ext['l']);
    this.batch = _b(ext['m']);
    this.brokerName = ext['n'] != null ? ext['n'] : null;
  }
}

export class ReplyMessageRequestHeader extends CommandCustomHeader {
  producerGroup: string | null = null;
  topic: string | null = null;
  defaultTopic: string | null = null;
  defaultTopicQueueNums: number | null = null;
  queueId: number | null = null;
  sysFlag: number | null = null;
  bornTimestamp: number | null = null;
  flag: number | null = null;
  properties: string | null = null;
  reconsumeTimes: number | null = null;
  unitMode: boolean | null = null;
  bornHost: string | null = null;
  storeHost: string | null = null;
  storeTimestamp: number | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      producerGroup: this.producerGroup,
      topic: this.topic,
      defaultTopic: this.defaultTopic,
      defaultTopicQueueNums: this.defaultTopicQueueNums,
      queueId: this.queueId,
      sysFlag: this.sysFlag,
      bornTimestamp: this.bornTimestamp,
      flag: this.flag,
      properties: this.properties,
      reconsumeTimes: this.reconsumeTimes,
      unitMode: this.unitMode,
      bornHost: this.bornHost,
      storeHost: this.storeHost,
      storeTimestamp: this.storeTimestamp,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.producerGroup = ext['producerGroup'] != null ? ext['producerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.defaultTopic = ext['defaultTopic'] != null ? ext['defaultTopic'] : null;
    this.defaultTopicQueueNums = _num(ext['defaultTopicQueueNums']);
    this.queueId = _num(ext['queueId']);
    this.sysFlag = _num(ext['sysFlag']);
    this.bornTimestamp = _num(ext['bornTimestamp']);
    this.flag = _num(ext['flag']);
    this.properties = ext['properties'] != null ? ext['properties'] : null;
    this.reconsumeTimes = _num(ext['reconsumeTimes']);
    this.unitMode = _b(ext['unitMode']);
    this.bornHost = ext['bornHost'] != null ? ext['bornHost'] : null;
    this.storeHost = ext['storeHost'] != null ? ext['storeHost'] : null;
    this.storeTimestamp = _num(ext['storeTimestamp']);
  }
}

export class SendMessageResponseHeader extends CommandCustomHeader {
  msgId: string | null = null;
  queueId: number | null = null;
  queueOffset: number | null = null;
  transactionId: string | null = null;
  batchUniqId: string | null = null;
  recallHandle: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      msgId: this.msgId,
      queueId: this.queueId,
      queueOffset: this.queueOffset,
      transactionId: this.transactionId,
      batchUniqId: this.batchUniqId,
      recallHandle: this.recallHandle,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.msgId = ext['msgId'] != null ? ext['msgId'] : null;
    this.queueId = _num(ext['queueId']);
    this.queueOffset = _num(ext['queueOffset']);
    this.transactionId = ext['transactionId'] != null ? ext['transactionId'] : null;
    this.batchUniqId = ext['batchUniqId'] != null ? ext['batchUniqId'] : null;
    this.recallHandle = ext['recallHandle'] != null ? ext['recallHandle'] : null;
  }
}

export class RecallMessageRequestHeader extends CommandCustomHeader {
  producerGroup: string | null = null;
  topic: string | null = null;
  recallHandle: string | null = null;
  bname: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({ producerGroup: this.producerGroup, topic: this.topic, recallHandle: this.recallHandle, bname: this.bname });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.producerGroup = ext['producerGroup'] != null ? ext['producerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.recallHandle = ext['recallHandle'] != null ? ext['recallHandle'] : null;
    this.bname = ext['bname'] != null ? ext['bname'] : null;
  }
}

export class RecallMessageResponseHeader extends CommandCustomHeader {
  msgId: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ msgId: this.msgId }); }
  fromExtFields(ext: Record<string, any>): void { this.msgId = ext['msgId'] != null ? ext['msgId'] : null; }
}

export class PullMessageRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  topic: string | null = null;
  liteTopic: string | null = null;
  queueId: number | null = null;
  queueOffset: number | null = null;
  maxMsgNums: number | null = null;
  sysFlag: number | null = null;
  commitOffset: number | null = null;
  suspendTimeoutMillis: number | null = null;
  subscription: string | null = null;
  subVersion: number | null = null;
  expressionType: string | null = null;
  maxMsgBytes: number | null = null;
  requestSource: number | null = null;
  proxyFrowardClientId: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      consumerGroup: this.consumerGroup,
      topic: this.topic,
      liteTopic: this.liteTopic,
      queueId: this.queueId,
      queueOffset: this.queueOffset,
      maxMsgNums: this.maxMsgNums,
      sysFlag: this.sysFlag,
      commitOffset: this.commitOffset,
      suspendTimeoutMillis: this.suspendTimeoutMillis,
      subscription: this.subscription,
      subVersion: this.subVersion,
      expressionType: this.expressionType,
      maxMsgBytes: this.maxMsgBytes,
      requestSource: this.requestSource,
      proxyFrowardClientId: this.proxyFrowardClientId,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.liteTopic = ext['liteTopic'] != null ? ext['liteTopic'] : null;
    this.queueId = _num(ext['queueId']);
    this.queueOffset = _num(ext['queueOffset']);
    this.maxMsgNums = _num(ext['maxMsgNums']);
    this.sysFlag = _num(ext['sysFlag']);
    this.commitOffset = _num(ext['commitOffset']);
    this.suspendTimeoutMillis = _num(ext['suspendTimeoutMillis']);
    this.subscription = ext['subscription'] != null ? ext['subscription'] : null;
    this.subVersion = _num(ext['subVersion']);
    this.expressionType = ext['expressionType'] != null ? ext['expressionType'] : null;
    this.maxMsgBytes = _num(ext['maxMsgBytes']);
    this.requestSource = _num(ext['requestSource']);
    this.proxyFrowardClientId = ext['proxyFrowardClientId'] != null ? ext['proxyFrowardClientId'] : null;
  }
}

export class PullMessageResponseHeader extends CommandCustomHeader {
  nextBeginOffset: number | null = null;
  minOffset: number | null = null;
  maxOffset: number | null = null;
  suggestWhichBrokerId: number | null = null;
  topicSysFlag: number | null = null;
  groupSysFlag: number | null = null;
  forbiddenType: number | null = null;
  offsetDelta: number | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      nextBeginOffset: this.nextBeginOffset,
      minOffset: this.minOffset,
      maxOffset: this.maxOffset,
      suggestWhichBrokerId: this.suggestWhichBrokerId,
      topicSysFlag: this.topicSysFlag,
      groupSysFlag: this.groupSysFlag,
      forbiddenType: this.forbiddenType,
      offsetDelta: this.offsetDelta,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.nextBeginOffset = _num(ext['nextBeginOffset']);
    this.minOffset = _num(ext['minOffset']);
    this.maxOffset = _num(ext['maxOffset']);
    this.suggestWhichBrokerId = _num(ext['suggestWhichBrokerId']);
    this.topicSysFlag = _num(ext['topicSysFlag']);
    this.groupSysFlag = _num(ext['groupSysFlag']);
    this.forbiddenType = _num(ext['forbiddenType']);
    this.offsetDelta = _num(ext['offsetDelta']);
  }
}

export class QueryConsumerOffsetRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  topic: string | null = null;
  queueId: number | null = null;
  setZeroIfNotFound: boolean | null = null;

  toExtFields(): Record<string, any> {
    return _ext({ consumerGroup: this.consumerGroup, topic: this.topic, queueId: this.queueId, setZeroIfNotFound: this.setZeroIfNotFound });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.setZeroIfNotFound = _b(ext['setZeroIfNotFound']);
  }
}

export class QueryConsumerOffsetResponseHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class UpdateConsumerOffsetRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  topic: string | null = null;
  queueId: number | null = null;
  commitOffset: number | null = null;

  toExtFields(): Record<string, any> {
    return _ext({ consumerGroup: this.consumerGroup, topic: this.topic, queueId: this.queueId, commitOffset: this.commitOffset });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.commitOffset = _num(ext['commitOffset']);
  }
}

export class UpdateConsumerOffsetResponseHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class GetMaxOffsetRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  queueId: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, queueId: this.queueId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
  }
}

export class GetMaxOffsetResponseHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class GetMinOffsetRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  queueId: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, queueId: this.queueId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
  }
}

export class GetMinOffsetResponseHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class SearchOffsetRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  queueId: number | null = null;
  timestamp: number | null = null;
  boundaryType: any = null;

  toExtFields(): Record<string, any> {
    return _ext({
      topic: this.topic,
      queueId: this.queueId,
      timestamp: this.timestamp,
      boundaryType: this.boundaryType != null ? this.boundaryType.value : null,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.timestamp = _num(ext['timestamp']);
    const value = ext['boundaryType'];
    this.boundaryType = value != null ? BoundaryType.get_type(value) : null;
  }
}

export class SearchOffsetResponseHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class GetEarliestMsgStoretimeRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  queueId: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, queueId: this.queueId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
  }
}

export class GetEarliestMsgStoretimeResponseHeader extends CommandCustomHeader {
  timestamp: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ timestamp: this.timestamp }); }
  fromExtFields(ext: Record<string, any>): void { this.timestamp = _num(ext['timestamp']); }
}

export class QueryMessageRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  key: string | null = null;
  maxNum: number | null = null;
  beginTimestamp: number | null = null;
  endTimestamp: number | null = null;
  indexType: string | null = null;
  lastKey: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      topic: this.topic, key: this.key, maxNum: this.maxNum,
      beginTimestamp: this.beginTimestamp, endTimestamp: this.endTimestamp,
      indexType: this.indexType, lastKey: this.lastKey,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.key = ext['key'] != null ? ext['key'] : null;
    this.maxNum = _num(ext['maxNum']);
    this.beginTimestamp = _num(ext['beginTimestamp']);
    this.endTimestamp = _num(ext['endTimestamp']);
    this.indexType = ext['indexType'] != null ? ext['indexType'] : null;
    this.lastKey = ext['lastKey'] != null ? ext['lastKey'] : null;
  }
}

export class QueryMessageResponseHeader extends CommandCustomHeader {
  indexLastUpdateTimestamp: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ indexLastUpdateTimestamp: this.indexLastUpdateTimestamp }); }
  fromExtFields(ext: Record<string, any>): void { this.indexLastUpdateTimestamp = _num(ext['indexLastUpdateTimestamp']); }
}

export class ViewMessageRequestHeader extends CommandCustomHeader {
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void { this.offset = _num(ext['offset']); }
}

export class ViewMessageResponseHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class HeartbeatRequestHeader extends CommandCustomHeader {
  clientID: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ clientID: this.clientID }); }
  fromExtFields(ext: Record<string, any>): void { this.clientID = ext['clientID'] != null ? ext['clientID'] : null; }
}

export class UnregisterClientRequestHeader extends CommandCustomHeader {
  clientID: string | null = null;
  producerGroup: string | null = null;
  consumerGroup: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({ clientID: this.clientID, producerGroup: this.producerGroup, consumerGroup: this.consumerGroup });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.clientID = ext['clientID'] != null ? ext['clientID'] : null;
    this.producerGroup = ext['producerGroup'] != null ? ext['producerGroup'] : null;
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
  }
}

export class ConsumerSendMsgBackRequestHeader extends CommandCustomHeader {
  offset: number | null = null;
  group: string | null = null;
  delayLevel: number | null = null;
  originMsgId: string | null = null;
  originTopic: string | null = null;
  unitMode: boolean | null = null;
  maxReconsumeTimes: number | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      offset: this.offset, group: this.group, delayLevel: this.delayLevel,
      originMsgId: this.originMsgId, originTopic: this.originTopic,
      unitMode: this.unitMode, maxReconsumeTimes: this.maxReconsumeTimes,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.offset = _num(ext['offset']);
    this.group = ext['group'] != null ? ext['group'] : null;
    this.delayLevel = _num(ext['delayLevel']);
    this.originMsgId = ext['originMsgId'] != null ? ext['originMsgId'] : null;
    this.originTopic = ext['originTopic'] != null ? ext['originTopic'] : null;
    this.unitMode = _b(ext['unitMode']);
    this.maxReconsumeTimes = _num(ext['maxReconsumeTimes']);
  }
}

export class GetConsumerListByGroupRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup }); }
  fromExtFields(ext: Record<string, any>): void { this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null; }
}

export class GetConsumerListByGroupResponseHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class NotifyConsumerIdsChangedRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup }); }
  fromExtFields(ext: Record<string, any>): void { this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null; }
}

export class GetConsumerConnectionListRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup }); }
  fromExtFields(ext: Record<string, any>): void { this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null; }
}

export class GetConsumerStatusRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  group: string | null = null;
  clientAddr: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, group: this.group, clientAddr: this.clientAddr }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.group = ext['group'] != null ? ext['group'] : null;
    this.clientAddr = ext['clientAddr'] != null ? ext['clientAddr'] : null;
  }
}

export class GetConsumerRunningInfoRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  clientId: string | null = null;
  jstackEnable: boolean | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup, clientId: this.clientId, jstackEnable: this.jstackEnable }); }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.clientId = ext['clientId'] != null ? ext['clientId'] : null;
    this.jstackEnable = _b(ext['jstackEnable']);
  }
}

export class ConsumeMessageDirectlyResultRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  clientId: string | null = null;
  msgId: string | null = null;
  brokerName: string | null = null;
  toExtFields(): Record<string, any> {
    return _ext({ consumerGroup: this.consumerGroup, clientId: this.clientId, msgId: this.msgId, brokerName: this.brokerName });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.clientId = ext['clientId'] != null ? ext['clientId'] : null;
    this.msgId = ext['msgId'] != null ? ext['msgId'] : null;
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
  }
}

export class ResetOffsetRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  group: string | null = null;
  timestamp: number | null = null;
  isForce: boolean | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, group: this.group, timestamp: this.timestamp, isForce: this.isForce }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.group = ext['group'] != null ? ext['group'] : null;
    this.timestamp = _num(ext['timestamp']);
    this.isForce = _b(ext['isForce']);
  }
}

export class LockBatchMqRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  clientId: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup, clientId: this.clientId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.clientId = ext['clientId'] != null ? ext['clientId'] : null;
  }
}

export class UnlockBatchMqRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  clientId: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup, clientId: this.clientId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.clientId = ext['clientId'] != null ? ext['clientId'] : null;
  }
}

export class EndTransactionRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  producerGroup: string | null = null;
  tranStateTableOffset: number | null = null;
  commitLogOffset: number | null = null;
  commitOrRollback: number | null = null;
  fromTransactionCheck: boolean | null = null;
  msgId: string | null = null;
  transactionId: string | null = null;
  bname: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      topic: this.topic, producerGroup: this.producerGroup,
      tranStateTableOffset: this.tranStateTableOffset, commitLogOffset: this.commitLogOffset,
      commitOrRollback: this.commitOrRollback, fromTransactionCheck: this.fromTransactionCheck,
      msgId: this.msgId, transactionId: this.transactionId, bname: this.bname,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.producerGroup = ext['producerGroup'] != null ? ext['producerGroup'] : null;
    this.tranStateTableOffset = _num(ext['tranStateTableOffset']);
    this.commitLogOffset = _num(ext['commitLogOffset']);
    this.commitOrRollback = _num(ext['commitOrRollback']);
    this.fromTransactionCheck = _b(ext['fromTransactionCheck']);
    this.msgId = ext['msgId'] != null ? ext['msgId'] : null;
    this.transactionId = ext['transactionId'] != null ? ext['transactionId'] : null;
    this.bname = ext['bname'] != null ? ext['bname'] : null;
  }
}

export class EndTransactionResponseHeader extends CommandCustomHeader {
  msgId: string | null = null;
  transactionId: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ msgId: this.msgId, transactionId: this.transactionId }); }
  fromExtFields(ext: Record<string, any>): void {
    this.msgId = ext['msgId'] != null ? ext['msgId'] : null;
    this.transactionId = ext['transactionId'] != null ? ext['transactionId'] : null;
  }
}

export class CheckTransactionStateRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  tranStateTableOffset: number | null = null;
  commitLogOffset: number | null = null;
  msgId: string | null = null;
  transactionId: string | null = null;
  offsetMsgId: string | null = null;
  bname: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      topic: this.topic, tranStateTableOffset: this.tranStateTableOffset,
      commitLogOffset: this.commitLogOffset, msgId: this.msgId,
      transactionId: this.transactionId, offsetMsgId: this.offsetMsgId, bname: this.bname,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.tranStateTableOffset = _num(ext['tranStateTableOffset']);
    this.commitLogOffset = _num(ext['commitLogOffset']);
    this.msgId = ext['msgId'] != null ? ext['msgId'] : null;
    this.transactionId = ext['transactionId'] != null ? ext['transactionId'] : null;
    this.offsetMsgId = ext['offsetMsgId'] != null ? ext['offsetMsgId'] : null;
    this.bname = ext['bname'] != null ? ext['bname'] : null;
  }
}

export class CheckTransactionStateResponseHeader extends CommandCustomHeader {
  groupName: string | null = null;
  transactionState: number | null = null;
  offset: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ groupName: this.groupName, transactionState: this.transactionState, offset: this.offset }); }
  fromExtFields(ext: Record<string, any>): void {
    this.groupName = ext['groupName'] != null ? ext['groupName'] : null;
    this.transactionState = _num(ext['transactionState']);
    this.offset = _num(ext['offset']);
  }
}

export class GetAllTopicConfigRequestHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class GetAllTopicConfigResponseHeader extends CommandCustomHeader {
  dataVersion: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ dataVersion: this.dataVersion }); }
  fromExtFields(ext: Record<string, any>): void { this.dataVersion = ext['dataVersion'] != null ? ext['dataVersion'] : null; }
}

export class GetTopicConfigRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void { this.topic = ext['topic'] != null ? ext['topic'] : null; }
}

export class CreateTopicRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  defaultTopic: string | null = null;
  readQueueNums: number | null = null;
  writeQueueNums: number | null = null;
  perm: number | null = null;
  topicFilterType: string | null = null;
  topicSysFlag: number | null = null;
  order: boolean | null = null;
  attributes: string | null = null;
  force: boolean | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      topic: this.topic, defaultTopic: this.defaultTopic,
      readQueueNums: this.readQueueNums, writeQueueNums: this.writeQueueNums,
      perm: this.perm, topicFilterType: this.topicFilterType,
      topicSysFlag: this.topicSysFlag, order: this.order,
      attributes: this.attributes, force: this.force,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.defaultTopic = ext['defaultTopic'] != null ? ext['defaultTopic'] : null;
    this.readQueueNums = _num(ext['readQueueNums']);
    this.writeQueueNums = _num(ext['writeQueueNums']);
    this.perm = _num(ext['perm']);
    this.topicFilterType = ext['topicFilterType'] != null ? ext['topicFilterType'] : null;
    this.topicSysFlag = _num(ext['topicSysFlag']);
    this.order = _b(ext['order']);
    this.attributes = ext['attributes'] != null ? ext['attributes'] : null;
    this.force = _b(ext['force']);
  }
}

export class DeleteTopicRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void { this.topic = ext['topic'] != null ? ext['topic'] : null; }
}

export class GetTopicStatsInfoRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void { this.topic = ext['topic'] != null ? ext['topic'] : null; }
}

export class GetConsumeStatsRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup, topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
  }
}

export class GetAllSubscriptionGroupConfigRequestHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class GetSubscriptionGroupConfigRequestHeader extends CommandCustomHeader {
  group: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ group: this.group }); }
  fromExtFields(ext: Record<string, any>): void { this.group = ext['group'] != null ? ext['group'] : null; }
}

export class InterviewGetConsumerStatusRequestHeader extends CommandCustomHeader {
  consumerGroup: string | null = null;
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ consumerGroup: this.consumerGroup, topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void {
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
  }
}

export class GetTopicsByClusterRequestHeader extends CommandCustomHeader {
  cluster: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ cluster: this.cluster }); }
  fromExtFields(ext: Record<string, any>): void { this.cluster = ext['cluster'] != null ? ext['cluster'] : null; }
}

export class GetBrokerConfigResponseHeader extends CommandCustomHeader {
  version: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ version: this.version }); }
  fromExtFields(ext: Record<string, any>): void { this.version = ext['version'] != null ? ext['version'] : null; }
}

export class GetTopicConfigResponseHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class GetSubscriptionGroupResponseHeader extends CommandCustomHeader {
  toExtFields(): Record<string, any> { return {}; }
  fromExtFields(_ext: Record<string, any>): void { /* no fields */ }
}

export class GetTopicListResponseHeader extends CommandCustomHeader {
  brokerAddr: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ brokerAddr: this.brokerAddr }); }
  fromExtFields(ext: Record<string, any>): void { this.brokerAddr = ext['brokerAddr'] != null ? ext['brokerAddr'] : null; }
}

export class RegisterBrokerRequestHeader extends CommandCustomHeader {
  brokerName: string | null = null;
  brokerAddr: string | null = null;
  clusterName: string | null = null;
  haServerAddr: string | null = null;
  brokerId: number | null = null;
  heartbeatTimeoutMillis: number | null = null;
  enableActingMaster: boolean | null = null;
  compressed: boolean = false;
  bodyCrc32: number = 0;

  toExtFields(): Record<string, any> {
    return _ext({
      brokerName: this.brokerName, brokerAddr: this.brokerAddr, clusterName: this.clusterName,
      haServerAddr: this.haServerAddr, brokerId: this.brokerId,
      heartbeatTimeoutMillis: this.heartbeatTimeoutMillis,
      enableActingMaster: this.enableActingMaster, compressed: this.compressed, bodyCrc32: this.bodyCrc32,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
    this.brokerAddr = ext['brokerAddr'] != null ? ext['brokerAddr'] : null;
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
    this.haServerAddr = ext['haServerAddr'] != null ? ext['haServerAddr'] : null;
    this.brokerId = _num(ext['brokerId']);
    this.heartbeatTimeoutMillis = _num(ext['heartbeatTimeoutMillis']);
    const ea = ext['enableActingMaster'];
    this.enableActingMaster = ea != null ? _b(ea) : null;
    this.compressed = ext['compressed'] === true || ext['compressed'] === 'true';
    this.bodyCrc32 = parseInt(ext['bodyCrc32'] != null ? ext['bodyCrc32'] : 0, 10);
  }
}

export class RegisterBrokerResponseHeader extends CommandCustomHeader {
  haServerAddr: string | null = null;
  masterAddr: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ haServerAddr: this.haServerAddr, masterAddr: this.masterAddr }); }
  fromExtFields(ext: Record<string, any>): void {
    this.haServerAddr = ext['haServerAddr'] != null ? ext['haServerAddr'] : null;
    this.masterAddr = ext['masterAddr'] != null ? ext['masterAddr'] : null;
  }
}

export class UnRegisterBrokerRequestHeader extends CommandCustomHeader {
  brokerName: string | null = null;
  brokerAddr: string | null = null;
  clusterName: string | null = null;
  brokerId: number | null = null;
  toExtFields(): Record<string, any> {
    return _ext({ brokerName: this.brokerName, brokerAddr: this.brokerAddr, clusterName: this.clusterName, brokerId: this.brokerId });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
    this.brokerAddr = ext['brokerAddr'] != null ? ext['brokerAddr'] : null;
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
    this.brokerId = _num(ext['brokerId']);
  }
}

export class GetRouteInfoRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  acceptStandardJsonOnly: boolean | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, acceptStandardJsonOnly: this.acceptStandardJsonOnly }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    const v = ext['acceptStandardJsonOnly'];
    this.acceptStandardJsonOnly = v != null ? _b(v) : null;
  }
}

export class PutKVConfigRequestHeader extends CommandCustomHeader {
  namespace: string | null = null;
  key: string | null = null;
  value: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ namespace: this.namespace, key: this.key, value: this.value }); }
  fromExtFields(ext: Record<string, any>): void {
    this.namespace = ext['namespace'] != null ? ext['namespace'] : null;
    this.key = ext['key'] != null ? ext['key'] : null;
    this.value = ext['value'] != null ? ext['value'] : null;
  }
}

export class GetKVConfigRequestHeader extends CommandCustomHeader {
  namespace: string | null = null;
  key: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ namespace: this.namespace, key: this.key }); }
  fromExtFields(ext: Record<string, any>): void {
    this.namespace = ext['namespace'] != null ? ext['namespace'] : null;
    this.key = ext['key'] != null ? ext['key'] : null;
  }
}

export class GetKVConfigResponseHeader extends CommandCustomHeader {
  value: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ value: this.value }); }
  fromExtFields(ext: Record<string, any>): void { this.value = ext['value'] != null ? ext['value'] : null; }
}

export class DeleteKVConfigRequestHeader extends CommandCustomHeader {
  namespace: string | null = null;
  key: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ namespace: this.namespace, key: this.key }); }
  fromExtFields(ext: Record<string, any>): void {
    this.namespace = ext['namespace'] != null ? ext['namespace'] : null;
    this.key = ext['key'] != null ? ext['key'] : null;
  }
}

export class GetKVListByNamespaceRequestHeader extends CommandCustomHeader {
  namespace: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ namespace: this.namespace }); }
  fromExtFields(ext: Record<string, any>): void { this.namespace = ext['namespace'] != null ? ext['namespace'] : null; }
}

export class RegisterTopicRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic }); }
  fromExtFields(ext: Record<string, any>): void { this.topic = ext['topic'] != null ? ext['topic'] : null; }
}

export class RegisterOrderTopicRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  orderTopicString: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, orderTopicString: this.orderTopicString }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.orderTopicString = ext['orderTopicString'] != null ? ext['orderTopicString'] : null;
  }
}

export class DeleteTopicFromNamesrvRequestHeader extends CommandCustomHeader {
  topic: string | null = null;
  clusterName: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ topic: this.topic, clusterName: this.clusterName }); }
  fromExtFields(ext: Record<string, any>): void {
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
  }
}

export class GetBrokerMemberGroupRequestHeader extends CommandCustomHeader {
  clusterName: string | null = null;
  brokerName: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ clusterName: this.clusterName, brokerName: this.brokerName }); }
  fromExtFields(ext: Record<string, any>): void {
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
  }
}

export class WipeWritePermOfBrokerRequestHeader extends CommandCustomHeader {
  brokerName: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ brokerName: this.brokerName }); }
  fromExtFields(ext: Record<string, any>): void { this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null; }
}

export class WipeWritePermOfBrokerResponseHeader extends CommandCustomHeader {
  wipeTopicCount: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ wipeTopicCount: this.wipeTopicCount }); }
  fromExtFields(ext: Record<string, any>): void { this.wipeTopicCount = _num(ext['wipeTopicCount']); }
}

export class AddWritePermOfBrokerRequestHeader extends CommandCustomHeader {
  brokerName: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ brokerName: this.brokerName }); }
  fromExtFields(ext: Record<string, any>): void { this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null; }
}

export class AddWritePermOfBrokerResponseHeader extends CommandCustomHeader {
  addTopicCount: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ addTopicCount: this.addTopicCount }); }
  fromExtFields(ext: Record<string, any>): void { this.addTopicCount = _num(ext['addTopicCount']); }
}

export class BrokerHeartbeatRequestHeader extends CommandCustomHeader {
  clusterName: string | null = null;
  brokerAddr: string | null = null;
  brokerName: string | null = null;
  brokerId: number | null = null;
  epoch: number | null = null;
  maxOffset: number | null = null;
  confirmOffset: number | null = null;
  heartbeatTimeoutMills: number | null = null;
  electionPriority: number | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      clusterName: this.clusterName, brokerAddr: this.brokerAddr, brokerName: this.brokerName,
      brokerId: this.brokerId, epoch: this.epoch, maxOffset: this.maxOffset,
      confirmOffset: this.confirmOffset, heartbeatTimeoutMills: this.heartbeatTimeoutMills,
      electionPriority: this.electionPriority,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
    this.brokerAddr = ext['brokerAddr'] != null ? ext['brokerAddr'] : null;
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
    this.brokerId = _num(ext['brokerId']);
    this.epoch = _num(ext['epoch']);
    this.maxOffset = _num(ext['maxOffset']);
    this.confirmOffset = _num(ext['confirmOffset']);
    this.heartbeatTimeoutMills = _num(ext['heartbeatTimeoutMills']);
    this.electionPriority = _num(ext['electionPriority']);
  }
}

export class QueryDataVersionRequestHeader extends CommandCustomHeader {
  brokerName: string | null = null;
  brokerAddr: string | null = null;
  clusterName: string | null = null;
  brokerId: number | null = null;
  toExtFields(): Record<string, any> {
    return _ext({ brokerName: this.brokerName, brokerAddr: this.brokerAddr, clusterName: this.clusterName, brokerId: this.brokerId });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.brokerName = ext['brokerName'] != null ? ext['brokerName'] : null;
    this.brokerAddr = ext['brokerAddr'] != null ? ext['brokerAddr'] : null;
    this.clusterName = ext['clusterName'] != null ? ext['clusterName'] : null;
    this.brokerId = _num(ext['brokerId']);
  }
}

export class QueryDataVersionResponseHeader extends CommandCustomHeader {
  changed: boolean | null = null;
  toExtFields(): Record<string, any> { return _ext({ changed: this.changed }); }
  fromExtFields(ext: Record<string, any>): void { this.changed = _b(ext['changed']); }
}

export class PopMessageRequestHeader extends CommandCustomHeader {
  // bname is the inherited RpcRequestHeader.bname; Java's popAsync sets it from
  // the message queue's broker name. The broker ignores it for POP, but it is
  // part of what Java sends.
  bname: string | null = null;
  consumerGroup: string | null = null;
  topic: string | null = null;
  queueId: number | null = null;
  maxMsgNums: number | null = null;
  invisibleTime: number | null = null;
  pollTime: number | null = null;
  bornTime: number | null = null;
  initMode: number | null = null;
  expType: string | null = null;
  exp: string | null = null;
  order = false;
  attemptId: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      bname: this.bname,
      consumerGroup: this.consumerGroup, topic: this.topic, queueId: this.queueId,
      maxMsgNums: this.maxMsgNums, invisibleTime: this.invisibleTime, pollTime: this.pollTime,
      bornTime: this.bornTime, initMode: this.initMode, expType: this.expType, exp: this.exp,
      order: this.order, attemptId: this.attemptId,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.bname = ext['bname'] != null ? ext['bname'] : null;
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.maxMsgNums = _num(ext['maxMsgNums']);
    this.invisibleTime = _num(ext['invisibleTime']);
    this.pollTime = _num(ext['pollTime']);
    this.bornTime = _num(ext['bornTime']);
    this.initMode = _num(ext['initMode']);
    this.expType = ext['expType'] != null ? ext['expType'] : null;
    this.exp = ext['exp'] != null ? ext['exp'] : null;
    this.order = ext['order'] === true || ext['order'] === 'true';
    this.attemptId = ext['attemptId'] != null ? ext['attemptId'] : null;
  }
}

export class PopMessageResponseHeader extends CommandCustomHeader {
  popTime: number | null = null;
  invisibleTime: number | null = null;
  reviveQid: number | null = null;
  restNum: number | null = null;
  startOffsetInfo: string | null = null;
  msgOffsetInfo: string | null = null;
  orderCountInfo: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      popTime: this.popTime, invisibleTime: this.invisibleTime, reviveQid: this.reviveQid,
      restNum: this.restNum, startOffsetInfo: this.startOffsetInfo,
      msgOffsetInfo: this.msgOffsetInfo, orderCountInfo: this.orderCountInfo,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.popTime = _num(ext['popTime']);
    this.invisibleTime = _num(ext['invisibleTime']);
    this.reviveQid = _num(ext['reviveQid']);
    this.restNum = _num(ext['restNum']);
    this.startOffsetInfo = ext['startOffsetInfo'] != null ? ext['startOffsetInfo'] : null;
    this.msgOffsetInfo = ext['msgOffsetInfo'] != null ? ext['msgOffsetInfo'] : null;
    this.orderCountInfo = ext['orderCountInfo'] != null ? ext['orderCountInfo'] : null;
  }
}

export class AckMessageRequestHeader extends CommandCustomHeader {
  bname: string | null = null;
  consumerGroup: string | null = null;
  topic: string | null = null;
  queueId: number | null = null;
  extraInfo: string | null = null;
  offset: number | null = null;
  liteTopic: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      bname: this.bname,
      consumerGroup: this.consumerGroup, topic: this.topic, queueId: this.queueId,
      extraInfo: this.extraInfo, offset: this.offset, liteTopic: this.liteTopic,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.bname = ext['bname'] != null ? ext['bname'] : null;
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.extraInfo = ext['extraInfo'] != null ? ext['extraInfo'] : null;
    this.offset = _num(ext['offset']);
    this.liteTopic = ext['liteTopic'] != null ? ext['liteTopic'] : null;
  }
}

export class ChangeInvisibleTimeRequestHeader extends CommandCustomHeader {
  bname: string | null = null;
  consumerGroup: string | null = null;
  topic: string | null = null;
  queueId: number | null = null;
  extraInfo: string | null = null;
  offset: number | null = null;
  invisibleTime: number | null = null;
  liteTopic: string | null = null;
  suspend = false;

  toExtFields(): Record<string, any> {
    return _ext({
      bname: this.bname,
      consumerGroup: this.consumerGroup, topic: this.topic, queueId: this.queueId,
      extraInfo: this.extraInfo, offset: this.offset, invisibleTime: this.invisibleTime,
      liteTopic: this.liteTopic, suspend: this.suspend,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.bname = ext['bname'] != null ? ext['bname'] : null;
    this.consumerGroup = ext['consumerGroup'] != null ? ext['consumerGroup'] : null;
    this.topic = ext['topic'] != null ? ext['topic'] : null;
    this.queueId = _num(ext['queueId']);
    this.extraInfo = ext['extraInfo'] != null ? ext['extraInfo'] : null;
    this.offset = _num(ext['offset']);
    this.invisibleTime = _num(ext['invisibleTime']);
    this.liteTopic = ext['liteTopic'] != null ? ext['liteTopic'] : null;
    this.suspend = ext['suspend'] === true || ext['suspend'] === 'true';
  }
}

export class ChangeInvisibleTimeResponseHeader extends CommandCustomHeader {
  popTime: number | null = null;
  invisibleTime: number | null = null;
  reviveQid: number | null = null;
  toExtFields(): Record<string, any> { return _ext({ popTime: this.popTime, invisibleTime: this.invisibleTime, reviveQid: this.reviveQid }); }
  fromExtFields(ext: Record<string, any>): void {
    this.popTime = _num(ext['popTime']);
    this.invisibleTime = _num(ext['invisibleTime']);
    this.reviveQid = _num(ext['reviveQid']);
  }
}

// Admin ACL headers (AUTH_CREATE_ACL / AUTH_UPDATE_ACL / AUTH_DELETE_ACL).
export class CreateAccessConfigRequestHeader extends CommandCustomHeader {
  accessKey: string | null = null;
  secretKey: string | null = null;
  whiteRemoteAddress: string | null = null;
  admin: boolean | null = null;
  defaultTopicPerm: string | null = null;
  defaultGroupPerm: string | null = null;
  topicPerms: string | null = null;
  groupPerms: string | null = null;

  toExtFields(): Record<string, any> {
    return _ext({
      accessKey: this.accessKey, secretKey: this.secretKey,
      whiteRemoteAddress: this.whiteRemoteAddress, admin: this.admin,
      defaultTopicPerm: this.defaultTopicPerm, defaultGroupPerm: this.defaultGroupPerm,
      topicPerms: this.topicPerms, groupPerms: this.groupPerms,
    });
  }
  fromExtFields(ext: Record<string, any>): void {
    this.accessKey = ext['accessKey'] != null ? ext['accessKey'] : null;
    this.secretKey = ext['secretKey'] != null ? ext['secretKey'] : null;
    this.whiteRemoteAddress = ext['whiteRemoteAddress'] != null ? ext['whiteRemoteAddress'] : null;
    this.admin = _b(ext['admin']);
    this.defaultTopicPerm = ext['defaultTopicPerm'] != null ? ext['defaultTopicPerm'] : null;
    this.defaultGroupPerm = ext['defaultGroupPerm'] != null ? ext['defaultGroupPerm'] : null;
    this.topicPerms = ext['topicPerms'] != null ? ext['topicPerms'] : null;
    this.groupPerms = ext['groupPerms'] != null ? ext['groupPerms'] : null;
  }
}

export class UpdateAccessConfigRequestHeader extends CreateAccessConfigRequestHeader {}

export class DeleteAccessConfigRequestHeader extends CommandCustomHeader {
  accessKey: string | null = null;
  toExtFields(): Record<string, any> { return _ext({ accessKey: this.accessKey }); }
  fromExtFields(ext: Record<string, any>): void { this.accessKey = ext['accessKey'] != null ? ext['accessKey'] : null; }
}

export default {
  CommandCustomHeader, BoundaryType,
  SendMessageRequestHeader, SendMessageRequestHeaderV2, ReplyMessageRequestHeader,
  SendMessageResponseHeader, RecallMessageRequestHeader, RecallMessageResponseHeader,
  PullMessageRequestHeader, PullMessageResponseHeader,
  QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
  UpdateConsumerOffsetRequestHeader, UpdateConsumerOffsetResponseHeader,
  GetMaxOffsetRequestHeader, GetMaxOffsetResponseHeader,
  GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
  SearchOffsetRequestHeader, SearchOffsetResponseHeader,
  GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
  QueryMessageRequestHeader, QueryMessageResponseHeader,
  ViewMessageRequestHeader, ViewMessageResponseHeader,
  HeartbeatRequestHeader, UnregisterClientRequestHeader,
  ConsumerSendMsgBackRequestHeader, GetConsumerListByGroupRequestHeader,
  GetConsumerListByGroupResponseHeader, NotifyConsumerIdsChangedRequestHeader,
  GetConsumerConnectionListRequestHeader, GetConsumerStatusRequestHeader,
  GetConsumerRunningInfoRequestHeader, ConsumeMessageDirectlyResultRequestHeader,
  ResetOffsetRequestHeader, LockBatchMqRequestHeader, UnlockBatchMqRequestHeader,
  EndTransactionRequestHeader, EndTransactionResponseHeader,
  CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader,
  GetAllTopicConfigRequestHeader, GetAllTopicConfigResponseHeader,
  GetTopicConfigRequestHeader, CreateTopicRequestHeader, DeleteTopicRequestHeader,
  GetTopicStatsInfoRequestHeader, GetConsumeStatsRequestHeader,
  GetAllSubscriptionGroupConfigRequestHeader, GetSubscriptionGroupConfigRequestHeader,
  InterviewGetConsumerStatusRequestHeader, GetTopicsByClusterRequestHeader,
  GetBrokerConfigResponseHeader, GetTopicConfigResponseHeader,
  GetSubscriptionGroupResponseHeader, GetTopicListResponseHeader,
  RegisterBrokerRequestHeader, RegisterBrokerResponseHeader, UnRegisterBrokerRequestHeader,
  GetRouteInfoRequestHeader, PutKVConfigRequestHeader, GetKVConfigRequestHeader,
  GetKVConfigResponseHeader, DeleteKVConfigRequestHeader, GetKVListByNamespaceRequestHeader,
  RegisterTopicRequestHeader, RegisterOrderTopicRequestHeader, DeleteTopicFromNamesrvRequestHeader,
  GetBrokerMemberGroupRequestHeader, WipeWritePermOfBrokerRequestHeader,
  WipeWritePermOfBrokerResponseHeader, AddWritePermOfBrokerRequestHeader,
  AddWritePermOfBrokerResponseHeader, BrokerHeartbeatRequestHeader,
  QueryDataVersionRequestHeader, QueryDataVersionResponseHeader,
  PopMessageRequestHeader, PopMessageResponseHeader, AckMessageRequestHeader,
  ChangeInvisibleTimeRequestHeader, ChangeInvisibleTimeResponseHeader,
  CreateAccessConfigRequestHeader, UpdateAccessConfigRequestHeader, DeleteAccessConfigRequestHeader,
};
