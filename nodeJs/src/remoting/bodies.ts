// -*- coding: utf-8 -*-
// Common response/request bodies (org.apache.rocketmq.remoting.protocol.body.*).
// Mirrors python/rocketmq/remoting/protocol/body.py, admin_body.py (shared parts),
// and the Java admin bodies. Many of these bodies contain Map<MessageQueue, X>
// whose JSON keys are inline-object keys (fastjson2 non-string keys); we use the
// fastjson-tolerant RemotingSerializable.decode and the messageQueueKey helpers to
// round-trip them. Field names match Java exactly.
import { MessageQueue } from '../common/message.ts';
import { RemotingSerializable, decodeMessageQueueKey } from './serialize.ts';
import { BrokerData, QueueData, TopicRouteData, brokerData2Json, topicRouteData2TopicRouteDataJson } from './route.ts';
import { SubscriptionGroupWrapper } from './subscription.ts';

// Re-export route topic helpers so a single import surface covers bodies + route.
export { BrokerData, QueueData, TopicRouteData, brokerData2Json, topicRouteData2TopicRouteDataJson };
// Re-export subscription wrapper for callers that only import bodies.
export { SubscriptionGroupWrapper };

// ---------------------------------------------------------------------------
// MessageQueue <-> inline JSON key helpers (mirror admin_body.py).
// ---------------------------------------------------------------------------
export function messageQueueKey(mq: MessageQueue): string {
  return `{"brokerName":"${mq.brokerName}","queueId":${mq.queueId},"topic":"${mq.topic}"}`;
}

export function parseMessageQueueKey(key: string): MessageQueue | null {
  const d = decodeMessageQueueKey(key);
  if (d == null) return null;
  return new MessageQueue(
    d['topic'] != null ? d['topic'] : '',
    d['brokerName'] != null ? d['brokerName'] : '',
    d['queueId'] != null ? d['queueId'] : 0,
  );
}

export function decodeMessageQueueMap(raw: Record<string, any> | null | undefined): Map<MessageQueue, any> {
  const result = new Map<MessageQueue, any>();
  const obj = raw || {};
  for (const [k, v] of Object.entries(obj)) {
    const mq = parseMessageQueueKey(k);
    if (mq != null) result.set(mq, v);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Simple bodies
// ---------------------------------------------------------------------------
export class KVTable {
  table: Record<string, string>;
  constructor() { this.table = {}; }
  toDict(): Record<string, any> { return { table: this.table }; }
  static fromDict(d: Record<string, any>): KVTable {
    const kv = new KVTable();
    kv.table = Object.assign({}, d['table'] || {});
    return kv;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): KVTable { return KVTable.fromDict(RemotingSerializable.decode(data) as Record<string, any>); }
}

export class TopicList {
  topicList: string[];
  brokerAddr: string | null;
  constructor() { this.topicList = []; this.brokerAddr = null; }
  toDict(): Record<string, any> {
    const d: Record<string, any> = { topicList: this.topicList };
    if (this.brokerAddr != null) d['brokerAddr'] = this.brokerAddr;
    return d;
  }
  static fromDict(d: Record<string, any>): TopicList {
    const tl = new TopicList();
    tl.topicList = (d['topicList'] || []).slice();
    tl.brokerAddr = d['brokerAddr'] != null ? d['brokerAddr'] : null;
    return tl;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): TopicList { return TopicList.fromDict(RemotingSerializable.decode(data) as Record<string, any>); }
}

export class GroupList {
  groupList: string[];
  constructor() { this.groupList = []; }
  toDict(): Record<string, any> { return { groupList: this.groupList }; }
  static fromDict(d: Record<string, any>): GroupList {
    const gl = new GroupList();
    gl.groupList = (d['groupList'] || []).slice();
    return gl;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): GroupList { return GroupList.fromDict(RemotingSerializable.decode(data) as Record<string, any>); }
}

export class LockBatchRequestBody {
  consumerGroup: string | null;
  clientId: string | null;
  mqSet: Record<string, any>[];
  constructor() { this.consumerGroup = null; this.clientId = null; this.mqSet = []; }
  toDict(): Record<string, any> {
    return { consumerGroup: this.consumerGroup, clientId: this.clientId, mqSet: this.mqSet };
  }
  static fromDict(d: Record<string, any>): LockBatchRequestBody {
    const b = new LockBatchRequestBody();
    b.consumerGroup = d['consumerGroup'] != null ? d['consumerGroup'] : null;
    b.clientId = d['clientId'] != null ? d['clientId'] : null;
    b.mqSet = (d['mqSet'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): LockBatchRequestBody {
    return LockBatchRequestBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class LockBatchResponseBody {
  lockOKMQSet: Record<string, any>[];
  constructor() { this.lockOKMQSet = []; }
  toDict(): Record<string, any> { return { lockOKMQSet: this.lockOKMQSet }; }
  static fromDict(d: Record<string, any>): LockBatchResponseBody {
    const b = new LockBatchResponseBody();
    b.lockOKMQSet = (d['lockOKMQSet'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): LockBatchResponseBody {
    return LockBatchResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class UnlockBatchRequestBody {
  consumerGroup: string | null;
  clientId: string | null;
  mqSet: Record<string, any>[];
  constructor() { this.consumerGroup = null; this.clientId = null; this.mqSet = []; }
  toDict(): Record<string, any> {
    return { consumerGroup: this.consumerGroup, clientId: this.clientId, mqSet: this.mqSet };
  }
  static fromDict(d: Record<string, any>): UnlockBatchRequestBody {
    const b = new UnlockBatchRequestBody();
    b.consumerGroup = d['consumerGroup'] != null ? d['consumerGroup'] : null;
    b.clientId = d['clientId'] != null ? d['clientId'] : null;
    b.mqSet = (d['mqSet'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): UnlockBatchRequestBody {
    return UnlockBatchRequestBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class GetConsumerListByGroupResponseBody {
  consumerIdList: string[];
  constructor() { this.consumerIdList = []; }
  toDict(): Record<string, any> { return { consumerIdList: this.consumerIdList }; }
  static fromDict(d: Record<string, any>): GetConsumerListByGroupResponseBody {
    const b = new GetConsumerListByGroupResponseBody();
    b.consumerIdList = (d['consumerIdList'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): GetConsumerListByGroupResponseBody {
    return GetConsumerListByGroupResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class CheckClientRequestBody {
  clientId: string | null;
  group: string | null;
  subscriptionData: Record<string, any> | null;
  namespace: string | null;
  constructor() { this.clientId = null; this.group = null; this.subscriptionData = null; this.namespace = null; }
  toDict(): Record<string, any> {
    const d: Record<string, any> = { clientId: this.clientId, group: this.group };
    if (this.subscriptionData != null) d['subscriptionData'] = this.subscriptionData;
    if (this.namespace != null) d['namespace'] = this.namespace;
    return d;
  }
  static fromDict(d: Record<string, any>): CheckClientRequestBody {
    const b = new CheckClientRequestBody();
    b.clientId = d['clientId'] != null ? d['clientId'] : null;
    b.group = d['group'] != null ? d['group'] : null;
    b.namespace = d['namespace'] != null ? d['namespace'] : null;
    b.subscriptionData = d['subscriptionData'] != null ? d['subscriptionData'] : null;
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): CheckClientRequestBody {
    return CheckClientRequestBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class ClusterInfo {
  brokerAddrTable: Record<string, Record<number, string>>;
  clusterAddrTable: Record<string, string[]>;
  constructor() { this.brokerAddrTable = {}; this.clusterAddrTable = {}; }
  getBrokerAddrs(): string[] {
    const addrs: string[] = [];
    const seen = new Set<string>();
    for (const name of Object.keys(this.brokerAddrTable).sort()) {
      const inner = this.brokerAddrTable[name];
      for (const id of Object.keys(inner).map((x) => parseInt(x, 10)).sort((a, b) => a - b)) {
        const addr = inner[id];
        if (addr && !seen.has(addr)) { seen.add(addr); addrs.push(addr); }
      }
    }
    return addrs;
  }
  toDict(): Record<string, any> {
    const bat: Record<string, any> = {};
    for (const [k, vv] of Object.entries(this.brokerAddrTable)) {
      const addrs: Record<string, string> = {};
      for (const [kk, v] of Object.entries(vv)) addrs[String(kk)] = v;
      bat[k] = { cluster: '', brokerName: k, brokerAddrs: addrs, enableActingMaster: false };
    }
    return { brokerAddrTable: bat, clusterAddrTable: this.clusterAddrTable };
  }
  static fromDict(d: Record<string, any>): ClusterInfo {
    const ci = new ClusterInfo();
    ci.brokerAddrTable = {};
    for (const [k, vv] of Object.entries(d['brokerAddrTable'] || {})) {
      const inner = (vv as any)['brokerAddrs'] || {};
      const addrs: Record<number, string> = {};
      for (const [kk, v] of Object.entries(inner)) addrs[parseInt(kk, 10)] = v as string;
      ci.brokerAddrTable[k] = addrs;
    }
    ci.clusterAddrTable = Object.assign({}, d['clusterAddrTable'] || {});
    return ci;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ClusterInfo { return ClusterInfo.fromDict(RemotingSerializable.decode(data) as Record<string, any>); }
}

export class Connection {
  clientId: string | null;
  clientAddr: string | null;
  language: string | null;
  version: number | null;
  constructor() { this.clientId = null; this.clientAddr = null; this.language = null; this.version = null; }
  toDict(): Record<string, any> {
    return { clientId: this.clientId, clientAddr: this.clientAddr, language: this.language, version: this.version };
  }
  static fromDict(d: Record<string, any>): Connection {
    const c = new Connection();
    c.clientId = d['clientId'] != null ? d['clientId'] : null;
    c.clientAddr = d['clientAddr'] != null ? d['clientAddr'] : null;
    c.language = d['language'] != null ? d['language'] : null;
    c.version = d['version'] != null ? d['version'] : null;
    return c;
  }
}

export class ConsumerConnection {
  connectionSet: Connection[];
  subscriptionTable: Record<string, any>;
  consumeType: string | null;
  messageModel: string | null;
  consumeFromWhere: string | null;
  constructor() {
    this.connectionSet = []; this.subscriptionTable = {}; this.consumeType = null;
    this.messageModel = null; this.consumeFromWhere = null;
  }
  toDict(): Record<string, any> {
    return {
      connectionSet: this.connectionSet.map((c) => c.toDict()),
      subscriptionTable: this.subscriptionTable,
      consumeType: this.consumeType,
      messageModel: this.messageModel,
      consumeFromWhere: this.consumeFromWhere,
    };
  }
  static fromDict(d: Record<string, any>): ConsumerConnection {
    const cc = new ConsumerConnection();
    cc.connectionSet = (d['connectionSet'] || []).map((c: any) => Connection.fromDict(c));
    cc.subscriptionTable = Object.assign({}, d['subscriptionTable'] || {});
    cc.consumeType = d['consumeType'] != null ? d['consumeType'] : null;
    cc.messageModel = d['messageModel'] != null ? d['messageModel'] : null;
    cc.consumeFromWhere = d['consumeFromWhere'] != null ? d['consumeFromWhere'] : null;
    return cc;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ConsumerConnection {
    return ConsumerConnection.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class ProducerConnection {
  connectionSet: Connection[];
  constructor() { this.connectionSet = []; }
  toDict(): Record<string, any> { return { connectionSet: this.connectionSet.map((c) => c.toDict()) }; }
  static fromDict(d: Record<string, any>): ProducerConnection {
    const pc = new ProducerConnection();
    pc.connectionSet = (d['connectionSet'] || []).map((c: any) => Connection.fromDict(c));
    return pc;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ProducerConnection {
    return ProducerConnection.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class QueryConsumeTimeSpanBody {
  consumeTimeSpanSet: Record<string, any>[];
  constructor() { this.consumeTimeSpanSet = []; }
  toDict(): Record<string, any> { return { consumeTimeSpanSet: this.consumeTimeSpanSet }; }
  static fromDict(d: Record<string, any>): QueryConsumeTimeSpanBody {
    const b = new QueryConsumeTimeSpanBody();
    b.consumeTimeSpanSet = (d['consumeTimeSpanSet'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): QueryConsumeTimeSpanBody {
    return QueryConsumeTimeSpanBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class ConsumeStatus {
  pullRT: number;
  pullTPS: number;
  consumeRT: number;
  consumeOKTPS: number;
  consumeFailedTPS: number;
  consumeFailedMsgs: number;
  constructor() {
    this.pullRT = 0; this.pullTPS = 0; this.consumeRT = 0; this.consumeOKTPS = 0;
    this.consumeFailedTPS = 0; this.consumeFailedMsgs = 0;
  }
  toDict(): Record<string, any> {
    return {
      pullRT: this.pullRT, pullTPS: this.pullTPS, consumeRT: this.consumeRT,
      consumeOKTPS: this.consumeOKTPS, consumeFailedTPS: this.consumeFailedTPS,
      consumeFailedMsgs: this.consumeFailedMsgs,
    };
  }
  static fromDict(d: Record<string, any>): ConsumeStatus {
    const cs = new ConsumeStatus();
    cs.pullRT = d['pullRT'] != null ? d['pullRT'] : 0;
    cs.pullTPS = d['pullTPS'] != null ? d['pullTPS'] : 0;
    cs.consumeRT = d['consumeRT'] != null ? d['consumeRT'] : 0;
    cs.consumeOKTPS = d['consumeOKTPS'] != null ? d['consumeOKTPS'] : 0;
    cs.consumeFailedTPS = d['consumeFailedTPS'] != null ? d['consumeFailedTPS'] : 0;
    cs.consumeFailedMsgs = d['consumeFailedMsgs'] != null ? d['consumeFailedMsgs'] : 0;
    return cs;
  }
}

export class ConsumeStatsList {
  statsList: Record<string, any>[];
  brokerAddr: string | null;
  totalDiff: number;
  totalInflightDiff: number;
  constructor() { this.statsList = []; this.brokerAddr = null; this.totalDiff = 0; this.totalInflightDiff = 0; }
  toDict(): Record<string, any> {
    const d: Record<string, any> = { consumeStatsList: this.statsList };
    if (this.brokerAddr != null) d['brokerAddr'] = this.brokerAddr;
    d['totalDiff'] = this.totalDiff;
    d['totalInflightDiff'] = this.totalInflightDiff;
    return d;
  }
  static fromDict(d: Record<string, any>): ConsumeStatsList {
    const sl = new ConsumeStatsList();
    sl.statsList = (d['consumeStatsList'] || []).slice();
    sl.brokerAddr = d['brokerAddr'] != null ? d['brokerAddr'] : null;
    sl.totalDiff = parseInt(d['totalDiff'] != null ? d['totalDiff'] : 0, 10);
    sl.totalInflightDiff = parseInt(d['totalInflightDiff'] != null ? d['totalInflightDiff'] : 0, 10);
    return sl;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ConsumeStatsList {
    return ConsumeStatsList.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class ResetOffsetBody {
  offsetTable: Map<MessageQueue, number>;
  constructor() { this.offsetTable = new Map(); }
  toDict(): Record<string, any> {
    const t: Record<string, any> = {};
    for (const [mq, v] of this.offsetTable) t[messageQueueKey(mq)] = v;
    return { offsetTable: t };
  }
  static fromDict(d: Record<string, any>): ResetOffsetBody {
    const b = new ResetOffsetBody();
    const m = decodeMessageQueueMap(d['offsetTable']);
    for (const [mq, v] of m) b.offsetTable.set(mq, parseInt(v, 10));
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ResetOffsetBody {
    return ResetOffsetBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class MessageQueueForC {
  topic: string;
  brokerName: string;
  queueId: number;
  offset: number;
  constructor(topic = '', brokerName = '', queueId = 0, offset = 0) {
    this.topic = topic; this.brokerName = brokerName; this.queueId = queueId; this.offset = offset;
  }
  toDict(): Record<string, any> {
    return { topic: this.topic, brokerName: this.brokerName, queueId: this.queueId, offset: this.offset };
  }
  static fromDict(d: Record<string, any>): MessageQueueForC {
    return new MessageQueueForC(
      d['topic'] != null ? d['topic'] : '',
      d['brokerName'] != null ? d['brokerName'] : '',
      parseInt(d['queueId'] != null ? d['queueId'] : 0, 10),
      parseInt(d['offset'] != null ? d['offset'] : 0, 10),
    );
  }
}

export class ResetOffsetBodyForC {
  offsetTable: MessageQueueForC[];
  constructor() { this.offsetTable = []; }
  toDict(): Record<string, any> { return { offsetTable: this.offsetTable.map((e) => e.toDict()) }; }
  static fromDict(d: Record<string, any>): ResetOffsetBodyForC {
    const b = new ResetOffsetBodyForC();
    for (const item of (d['offsetTable'] || [])) {
      if (item && typeof item === 'object') b.offsetTable.push(MessageQueueForC.fromDict(item));
    }
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ResetOffsetBodyForC {
    return ResetOffsetBodyForC.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class GetConsumerStatusBody {
  messageQueueTable: Map<MessageQueue, number>;
  consumerTable: Record<string, Map<MessageQueue, number>>;
  constructor() { this.messageQueueTable = new Map(); this.consumerTable = {}; }
  toDict(): Record<string, any> {
    const mqt: Record<string, any> = {};
    for (const [mq, v] of this.messageQueueTable) mqt[messageQueueKey(mq)] = v;
    const ct: Record<string, any> = {};
    for (const [cid, tbl] of Object.entries(this.consumerTable)) {
      const t: Record<string, any> = {};
      for (const [mq, v] of tbl) t[messageQueueKey(mq)] = v;
      ct[cid] = t;
    }
    return { messageQueueTable: mqt, consumerTable: ct };
  }
  static fromDict(d: Record<string, any>): GetConsumerStatusBody {
    const b = new GetConsumerStatusBody();
    const m = decodeMessageQueueMap(d['messageQueueTable']);
    for (const [mq, v] of m) b.messageQueueTable.set(mq, parseInt(v, 10));
    for (const [cid, tbl] of Object.entries(d['consumerTable'] || {})) {
      const inner = new Map<MessageQueue, number>();
      const m2 = decodeMessageQueueMap(tbl as Record<string, any>);
      for (const [mq, v] of m2) inner.set(mq, parseInt(v, 10));
      b.consumerTable[cid] = inner;
    }
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): GetConsumerStatusBody {
    return GetConsumerStatusBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class ProcessQueueInfo {
  commitOffset: number;
  cachedMsgMinOffset: number;
  cachedMsgMaxOffset: number;
  cachedMsgCount: number;
  cachedMsgSizeInMiB: number;
  transactionMsgMinOffset: number;
  transactionMsgMaxOffset: number;
  transactionMsgCount: number;
  locked: boolean;
  tryUnlockTimes: number;
  lastLockTimestamp: number;
  droped: boolean;
  lastPullTimestamp: number;
  lastConsumeTimestamp: number;
  constructor() {
    this.commitOffset = 0; this.cachedMsgMinOffset = 0; this.cachedMsgMaxOffset = 0; this.cachedMsgCount = 0;
    this.cachedMsgSizeInMiB = 0; this.transactionMsgMinOffset = 0; this.transactionMsgMaxOffset = 0;
    this.transactionMsgCount = 0; this.locked = false; this.tryUnlockTimes = 0; this.lastLockTimestamp = 0;
    this.droped = false; this.lastPullTimestamp = 0; this.lastConsumeTimestamp = 0;
  }
  toDict(): Record<string, any> {
    return {
      commitOffset: this.commitOffset, cachedMsgMinOffset: this.cachedMsgMinOffset,
      cachedMsgMaxOffset: this.cachedMsgMaxOffset, cachedMsgCount: this.cachedMsgCount,
      cachedMsgSizeInMiB: this.cachedMsgSizeInMiB, transactionMsgMinOffset: this.transactionMsgMinOffset,
      transactionMsgMaxOffset: this.transactionMsgMaxOffset, transactionMsgCount: this.transactionMsgCount,
      locked: this.locked, tryUnlockTimes: this.tryUnlockTimes, lastLockTimestamp: this.lastLockTimestamp,
      droped: this.droped, lastPullTimestamp: this.lastPullTimestamp, lastConsumeTimestamp: this.lastConsumeTimestamp,
    };
  }
  static fromDict(d: Record<string, any>): ProcessQueueInfo {
    const p = new ProcessQueueInfo();
    p.commitOffset = parseInt(d['commitOffset'] != null ? d['commitOffset'] : 0, 10);
    p.cachedMsgMinOffset = parseInt(d['cachedMsgMinOffset'] != null ? d['cachedMsgMinOffset'] : 0, 10);
    p.cachedMsgMaxOffset = parseInt(d['cachedMsgMaxOffset'] != null ? d['cachedMsgMaxOffset'] : 0, 10);
    p.cachedMsgCount = parseInt(d['cachedMsgCount'] != null ? d['cachedMsgCount'] : 0, 10);
    p.cachedMsgSizeInMiB = parseInt(d['cachedMsgSizeInMiB'] != null ? d['cachedMsgSizeInMiB'] : 0, 10);
    p.transactionMsgMinOffset = parseInt(d['transactionMsgMinOffset'] != null ? d['transactionMsgMinOffset'] : 0, 10);
    p.transactionMsgMaxOffset = parseInt(d['transactionMsgMaxOffset'] != null ? d['transactionMsgMaxOffset'] : 0, 10);
    p.transactionMsgCount = parseInt(d['transactionMsgCount'] != null ? d['transactionMsgCount'] : 0, 10);
    p.locked = d['locked'] === true;
    p.tryUnlockTimes = parseInt(d['tryUnlockTimes'] != null ? d['tryUnlockTimes'] : 0, 10);
    p.lastLockTimestamp = parseInt(d['lastLockTimestamp'] != null ? d['lastLockTimestamp'] : 0, 10);
    p.droped = d['droped'] === true;
    p.lastPullTimestamp = parseInt(d['lastPullTimestamp'] != null ? d['lastPullTimestamp'] : 0, 10);
    p.lastConsumeTimestamp = parseInt(d['lastConsumeTimestamp'] != null ? d['lastConsumeTimestamp'] : 0, 10);
    return p;
  }
}

export class CMResult {
  static CR_SUCCESS = 'CR_SUCCESS';
  static CR_LATER = 'CR_LATER';
  static CR_ROLLBACK = 'CR_ROLLBACK';
  static CR_COMMIT = 'CR_COMMIT';
  static CR_THROW_EXCEPTION = 'CR_THROW_EXCEPTION';
  static CR_RETURN_NULL = 'CR_RETURN_NULL';
}

export class ConsumeMessageDirectlyResult {
  order: boolean;
  autoCommit: boolean;
  consumeResult: string | null;
  remark: string | null;
  spentTimeMills: number;
  constructor() {
    this.order = false; this.autoCommit = true; this.consumeResult = null; this.remark = null; this.spentTimeMills = 0;
  }
  toDict(): Record<string, any> {
    return {
      order: this.order, autoCommit: this.autoCommit, consumeResult: this.consumeResult,
      remark: this.remark, spentTimeMills: this.spentTimeMills,
    };
  }
  static fromDict(d: Record<string, any>): ConsumeMessageDirectlyResult {
    const r = new ConsumeMessageDirectlyResult();
    r.order = d['order'] === true;
    r.autoCommit = d['autoCommit'] !== false;
    r.consumeResult = d['consumeResult'] != null ? d['consumeResult'] : null;
    r.remark = d['remark'] != null ? d['remark'] : null;
    r.spentTimeMills = parseInt(d['spentTimeMills'] != null ? d['spentTimeMills'] : 0, 10);
    return r;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ConsumeMessageDirectlyResult {
    return ConsumeMessageDirectlyResult.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------------------
// ConsumerRunningInfo (307 response). Properties + subscriptionSet + mqTable +
// mqPopTable + statusTable + userConsumerInfo + jstack.
// ---------------------------------------------------------------------------
export class ConsumerRunningInfo {
  static PROP_NAMESERVER_ADDR = 'PROP_NAMESERVER_ADDR';
  static PROP_THREADPOOL_CORE_SIZE = 'PROP_THREADPOOL_CORE_SIZE';
  static PROP_CONSUME_ORDERLY = 'PROP_CONSUMEORDERLY';
  static PROP_CONSUME_TYPE = 'PROP_CONSUME_TYPE';
  static PROP_CLIENT_VERSION = 'PROP_CLIENT_VERSION';
  static PROP_CONSUMER_START_TIMESTAMP = 'PROP_CONSUMER_START_TIMESTAMP';

  properties: Record<string, string>;
  subscriptionSet: Record<string, any>[];
  mqTable: Map<MessageQueue, Record<string, any>>;
  mqPopTable: Map<MessageQueue, Record<string, any>>;
  statusTable: Record<string, any>;
  userConsumerInfo: Record<string, string>;
  jstack: string | null;
  constructor() {
    this.properties = {}; this.subscriptionSet = []; this.mqTable = new Map();
    this.mqPopTable = new Map(); this.statusTable = {}; this.userConsumerInfo = {}; this.jstack = null;
  }
  toDict(): Record<string, any> {
    const mqt: Record<string, any> = {};
    for (const [mq, v] of this.mqTable) mqt[messageQueueKey(mq)] = v;
    const mpt: Record<string, any> = {};
    for (const [mq, v] of this.mqPopTable) mpt[messageQueueKey(mq)] = v;
    return {
      properties: this.properties,
      subscriptionSet: this.subscriptionSet,
      mqTable: mqt,
      mqPopTable: mpt,
      statusTable: this.statusTable,
      userConsumerInfo: this.userConsumerInfo,
      jstack: this.jstack,
    };
  }
  static fromDict(d: Record<string, any>): ConsumerRunningInfo {
    const ri = new ConsumerRunningInfo();
    ri.properties = Object.assign({}, d['properties'] || {});
    ri.subscriptionSet = (d['subscriptionSet'] || []).slice();
    const mqtMap = decodeMessageQueueMap(d['mqTable']);
    for (const [mq, v] of mqtMap) ri.mqTable.set(mq, v as Record<string, any>);
    const mptMap = decodeMessageQueueMap(d['mqPopTable']);
    for (const [mq, v] of mptMap) ri.mqPopTable.set(mq, v as Record<string, any>);
    ri.statusTable = Object.assign({}, d['statusTable'] || {});
    ri.userConsumerInfo = Object.assign({}, d['userConsumerInfo'] || {});
    ri.jstack = d['jstack'] != null ? d['jstack'] : null;
    return ri;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ConsumerRunningInfo {
    return ConsumerRunningInfo.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
  // Build a tps summary from statusTable, given a sampler that injects the two
  // timestamps (pull/consume) needed to compute throughput. `manager` is the
  // consumption status manager that exposes pullTps/consumeTps, etc.
  computeTps(manager?: { getTps?: (mq: MessageQueue, now: number) => number }): number {
    if (!manager || !manager.getTps) return 0;
    let total = 0;
    for (const mq of this.mqTable.keys()) total += manager.getTps(mq, Date.now());
    return total;
  }
}

// ---------------------------------------------------------------------------
// TopicConfig (shared by bodies + admin). Java defaults: readQueueNums=16,
// writeQueueNums=16, perm=6, topicFilterType=SINGLE_TAG, topicSysFlag=0,
// order=false, attributes={}.
// ---------------------------------------------------------------------------
export const TopicFilterType = {
  SINGLE_TAG: 'SINGLE_TAG',
  MULTI_TAG: 'MULTI_TAG',
};

export const DEFAULT_READ_QUEUE_NUMS = 16;
export const DEFAULT_WRITE_QUEUE_NUMS = 16;
export const DEFAULT_PERM = 6;

export class TopicConfig {
  topicName: string;
  readQueueNums: number;
  writeQueueNums: number;
  perm: number;
  topicFilterType: string;
  topicSysFlag: number;
  order: boolean;
  attributes: Record<string, string>;
  constructor(
    topicName = '',
    readQueueNums = DEFAULT_READ_QUEUE_NUMS,
    writeQueueNums = DEFAULT_WRITE_QUEUE_NUMS,
    perm = DEFAULT_PERM,
    topicFilterType = TopicFilterType.SINGLE_TAG,
    topicSysFlag = 0,
    order = false,
  ) {
    this.topicName = topicName;
    this.readQueueNums = readQueueNums;
    this.writeQueueNums = writeQueueNums;
    this.perm = perm;
    this.topicFilterType = topicFilterType;
    this.topicSysFlag = topicSysFlag;
    this.order = order;
    this.attributes = {};
  }
  toDict(): Record<string, any> {
    return {
      topicName: this.topicName,
      readQueueNums: this.readQueueNums,
      writeQueueNums: this.writeQueueNums,
      perm: this.perm,
      topicFilterType: this.topicFilterType,
      topicSysFlag: this.topicSysFlag,
      order: this.order,
      attributes: this.attributes,
    };
  }
  static fromDict(d: Record<string, any>): TopicConfig {
    return new TopicConfig(
      d['topicName'] != null ? d['topicName'] : '',
      parseInt(d['readQueueNums'] != null ? d['readQueueNums'] : DEFAULT_READ_QUEUE_NUMS, 10),
      parseInt(d['writeQueueNums'] != null ? d['writeQueueNums'] : DEFAULT_WRITE_QUEUE_NUMS, 10),
      parseInt(d['perm'] != null ? d['perm'] : DEFAULT_PERM, 10),
      d['topicFilterType'] != null ? d['topicFilterType'] : TopicFilterType.SINGLE_TAG,
      parseInt(d['topicSysFlag'] != null ? d['topicSysFlag'] : 0, 10),
      d['order'] === true,
    );
  }
  // NOTE: Java TopicConfig.readBody/writeBody are a proprietary binary KV codec
  // (used by UPDATE_AND_CREATE_TOPIC bodies); that codec is not part of the
  // foundation's RocketMQSerializable and is absent from the Python reference,
  // so we omit it here (encode/decode use JSON, consistent with the rest).
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): TopicConfig {
    return TopicConfig.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class TopicConfigSerializeWrapper {
  topicConfigTable: Record<string, TopicConfig>;
  dataVersion: Record<string, any>;
  constructor() { this.topicConfigTable = {}; this.dataVersion = {}; }
  toDict(): Record<string, any> {
    const table: Record<string, any> = {};
    for (const [k, v] of Object.entries(this.topicConfigTable)) table[k] = v.toDict();
    return { dataVersion: this.dataVersion, topicConfigTable: table };
  }
  static fromDict(d: Record<string, any>): TopicConfigSerializeWrapper {
    const w = new TopicConfigSerializeWrapper();
    const table = d['topicConfigTable'] || {};
    for (const [k, v] of Object.entries(table)) w.topicConfigTable[k] = TopicConfig.fromDict(v as Record<string, any>);
    w.dataVersion = Object.assign({}, d['dataVersion'] || {});
    return w;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): TopicConfigSerializeWrapper {
    return TopicConfigSerializeWrapper.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------------------
// ConsumeStats + OffsetWrapper (admin; shared with admin_body via re-export).
// ---------------------------------------------------------------------------
export class OffsetWrapper {
  brokerOffset: number;
  consumerOffset: number;
  lastTimestamp: number;
  pullOffset: number;
  constructor(brokerOffset = 0, consumerOffset = 0, lastTimestamp = 0, pullOffset = 0) {
    this.brokerOffset = brokerOffset; this.consumerOffset = consumerOffset;
    this.lastTimestamp = lastTimestamp; this.pullOffset = pullOffset;
  }
  get lag(): number { return this.brokerOffset - this.consumerOffset; }
  toDict(): Record<string, any> {
    return {
      brokerOffset: this.brokerOffset, consumerOffset: this.consumerOffset,
      lastTimestamp: this.lastTimestamp, pullOffset: this.pullOffset,
    };
  }
  static fromDict(d: Record<string, any>): OffsetWrapper {
    return new OffsetWrapper(
      parseInt(d['brokerOffset'] != null ? d['brokerOffset'] : 0, 10),
      parseInt(d['consumerOffset'] != null ? d['consumerOffset'] : 0, 10),
      parseInt(d['lastTimestamp'] != null ? d['lastTimestamp'] : 0, 10),
      parseInt(d['pullOffset'] != null ? d['pullOffset'] : 0, 10),
    );
  }
}

export class ConsumeStats {
  offsetTable: Map<MessageQueue, OffsetWrapper>;
  consumeTps: number;
  constructor() { this.offsetTable = new Map(); this.consumeTps = 0; }
  get totalLag(): number {
    let s = 0;
    for (const ow of this.offsetTable.values()) s += ow.lag;
    return s;
  }
  toDict(): Record<string, any> {
    const t: Record<string, any> = {};
    for (const [mq, v] of this.offsetTable) t[messageQueueKey(mq)] = v.toDict();
    return { offsetTable: t, consumeTps: this.consumeTps };
  }
  static fromDict(d: Record<string, any>): ConsumeStats {
    const cs = new ConsumeStats();
    const m = decodeMessageQueueMap(d['offsetTable']);
    for (const [mq, v] of m) cs.offsetTable.set(mq, OffsetWrapper.fromDict(v as Record<string, any>));
    cs.consumeTps = d['consumeTps'] != null ? d['consumeTps'] : 0;
    return cs;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): ConsumeStats {
    return ConsumeStats.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------------------
// BrokerStatsData + StatsItem (VIEW_BROKER_STATS_DATA).
// ---------------------------------------------------------------------------
export class StatsItem {
  statsName: string;
  statsKey: string;
  sum: number;
  tps: number;
  times: number;
  value: number;
  constructor() { this.statsName = ''; this.statsKey = ''; this.sum = 0; this.tps = 0; this.times = 0; this.value = 0; }
  toDict(): Record<string, any> {
    return { statsName: this.statsName, statsKey: this.statsKey, sum: this.sum, tps: this.tps, times: this.times, value: this.value };
  }
  static fromDict(d: Record<string, any>): StatsItem {
    const s = new StatsItem();
    s.statsName = d['statsName'] != null ? d['statsName'] : '';
    s.statsKey = d['statsKey'] != null ? d['statsKey'] : '';
    s.sum = d['sum'] != null ? d['sum'] : 0;
    s.tps = d['tps'] != null ? d['tps'] : 0;
    s.times = d['times'] != null ? d['times'] : 0;
    s.value = d['value'] != null ? d['value'] : 0;
    return s;
  }
}

export class BrokerStatsData {
  brokerPutNums: StatsItem;
  brokerGetNums: StatsItem;
  brokerGetSize: StatsItem;
  brokerSucPutTimes: StatsItem;
  brokerSucGetTimes: StatsItem;
  msgPutTotalYesterdayMorning: number;
  msgPutTotalTodayMorning: number;
  msgGetTotalYesterdayMorning: number;
  msgGetTotalTodayMorning: number;
  msgPutTotalTodayNow: number;
  msgGetTotalTodayNow: number;
  putTps: number[];
  getTps: number[];
  getTransferredTps: number[];
  constructor() {
    this.brokerPutNums = new StatsItem(); this.brokerGetNums = new StatsItem();
    this.brokerGetSize = new StatsItem(); this.brokerSucPutTimes = new StatsItem();
    this.brokerSucGetTimes = new StatsItem();
    this.msgPutTotalYesterdayMorning = 0; this.msgPutTotalTodayMorning = 0;
    this.msgGetTotalYesterdayMorning = 0; this.msgGetTotalTodayMorning = 0;
    this.msgPutTotalTodayNow = 0; this.msgGetTotalTodayNow = 0;
    this.putTps = []; this.getTps = []; this.getTransferredTps = [];
  }
  toDict(): Record<string, any> {
    return {
      brokerPutNums: this.brokerPutNums.toDict(), brokerGetNums: this.brokerGetNums.toDict(),
      brokerGetSize: this.brokerGetSize.toDict(), brokerSucPutTimes: this.brokerSucPutTimes.toDict(),
      brokerSucGetTimes: this.brokerSucGetTimes.toDict(),
      msgPutTotalYesterdayMorning: this.msgPutTotalYesterdayMorning,
      msgPutTotalTodayMorning: this.msgPutTotalTodayMorning,
      msgGetTotalYesterdayMorning: this.msgGetTotalYesterdayMorning,
      msgGetTotalTodayMorning: this.msgGetTotalTodayMorning,
      msgPutTotalTodayNow: this.msgPutTotalTodayNow, msgGetTotalTodayNow: this.msgGetTotalTodayNow,
      putTps: this.putTps, getTps: this.getTps, getTransferredTps: this.getTransferredTps,
    };
  }
  static fromDict(d: Record<string, any>): BrokerStatsData {
    const b = new BrokerStatsData();
    b.brokerPutNums = StatsItem.fromDict(d['brokerPutNums'] || {});
    b.brokerGetNums = StatsItem.fromDict(d['brokerGetNums'] || {});
    b.brokerGetSize = StatsItem.fromDict(d['brokerGetSize'] || {});
    b.brokerSucPutTimes = StatsItem.fromDict(d['brokerSucPutTimes'] || {});
    b.brokerSucGetTimes = StatsItem.fromDict(d['brokerSucGetTimes'] || {});
    b.msgPutTotalYesterdayMorning = parseInt(d['msgPutTotalYesterdayMorning'] != null ? d['msgPutTotalYesterdayMorning'] : 0, 10);
    b.msgPutTotalTodayMorning = parseInt(d['msgPutTotalTodayMorning'] != null ? d['msgPutTotalTodayMorning'] : 0, 10);
    b.msgGetTotalYesterdayMorning = parseInt(d['msgGetTotalYesterdayMorning'] != null ? d['msgGetTotalYesterdayMorning'] : 0, 10);
    b.msgGetTotalTodayMorning = parseInt(d['msgGetTotalTodayMorning'] != null ? d['msgGetTotalTodayMorning'] : 0, 10);
    b.msgPutTotalTodayNow = parseInt(d['msgPutTotalTodayNow'] != null ? d['msgPutTotalTodayNow'] : 0, 10);
    b.msgGetTotalTodayNow = parseInt(d['msgGetTotalTodayNow'] != null ? d['msgGetTotalTodayNow'] : 0, 10);
    b.putTps = (d['putTps'] || []).map((x: any) => Number(x));
    b.getTps = (d['getTps'] || []).map((x: any) => Number(x));
    b.getTransferredTps = (d['getTransferredTps'] || []).map((x: any) => Number(x));
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): BrokerStatsData {
    return BrokerStatsData.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// MessageRequestMode (SET_MESSAGE_REQUEST_MODE enum: PULL=0, POP=1).
export const MessageRequestMode = {
  PULL: 0,
  POP: 1,
};

// ---------------------------------------------------------------------------
// QueryMsgResponseBody (QUERY_MESSAGE response).
// ---------------------------------------------------------------------------
export class QueryMsgResponseBody {
  indexLastUpdateTimestamp: number;
  msgIdSet: string[];
  msgIdList: string[];
  constructor() { this.indexLastUpdateTimestamp = 0; this.msgIdSet = []; this.msgIdList = []; }
  toDict(): Record<string, any> {
    return {
      indexLastUpdateTimestamp: this.indexLastUpdateTimestamp,
      msgIdSet: this.msgIdSet, msgIdList: this.msgIdList,
    };
  }
  static fromDict(d: Record<string, any>): QueryMsgResponseBody {
    const b = new QueryMsgResponseBody();
    b.indexLastUpdateTimestamp = parseInt(d['indexLastUpdateTimestamp'] != null ? d['indexLastUpdateTimestamp'] : 0, 10);
    b.msgIdSet = (d['msgIdSet'] || d['msgIdList'] || []).slice();
    b.msgIdList = (d['msgIdList'] || d['msgIdSet'] || []).slice();
    return b;
  }
  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }
  static decode(data: Buffer): QueryMsgResponseBody {
    return QueryMsgResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// QueryResult (optional, lightweight): holds the index timestamp + raw messages.
export class QueryResult {
  indexLastUpdateTimestamp: number;
  messageList: any[];
  constructor() { this.indexLastUpdateTimestamp = 0; this.messageList = []; }
  toDict(): Record<string, any> {
    return { indexLastUpdateTimestamp: this.indexLastUpdateTimestamp, messageList: this.messageList };
  }
  static fromDict(d: Record<string, any>): QueryResult {
    const q = new QueryResult();
    q.indexLastUpdateTimestamp = parseInt(d['indexLastUpdateTimestamp'] != null ? d['indexLastUpdateTimestamp'] : 0, 10);
    q.messageList = (d['messageList'] || []).slice();
    return q;
  }
}

export default {
  KVTable, TopicList, GroupList, LockBatchRequestBody, LockBatchResponseBody, UnlockBatchRequestBody,
  GetConsumerListByGroupResponseBody, CheckClientRequestBody, ClusterInfo, Connection, ConsumerConnection,
  ProducerConnection, QueryConsumeTimeSpanBody, ConsumeStatus, ConsumeStatsList, ResetOffsetBody,
  MessageQueueForC, ResetOffsetBodyForC, GetConsumerStatusBody, ProcessQueueInfo, CMResult,
  ConsumeMessageDirectlyResult, ConsumerRunningInfo, TopicConfig, TopicConfigSerializeWrapper,
  OffsetWrapper, ConsumeStats, BrokerStatsData, StatsItem, MessageRequestMode, QueryMsgResponseBody, QueryResult,
  messageQueueKey, parseMessageQueueKey, decodeMessageQueueMap,
};
