// -*- coding: utf-8 -*-
import { MessageQueue } from '../common/message.ts';
import { RemotingSerializable } from './serialize.ts';
import {
  messageQueueKey,
  parseMessageQueueKey,
  decodeMessageQueueMap,
  TopicConfig,
  TopicConfigSerializeWrapper,
  OffsetWrapper,
  ConsumeStats,
  ConsumerRunningInfo,
} from './bodies.ts';

// Re-export shared admin bodies so a single import covers all admin body types.
export {
  messageQueueKey,
  parseMessageQueueKey,
  decodeMessageQueueMap,
  TopicConfig,
  TopicConfigSerializeWrapper,
  OffsetWrapper,
  ConsumeStats,
  ConsumerRunningInfo,
};

// ---------------------------------------------------------------- TopicOffset
export class TopicOffset {
  minOffset = 0;
  maxOffset = 0;
  lastUpdateTimestamp = 0;

  constructor(minOffset = 0, maxOffset = 0, lastUpdateTimestamp = 0) {
    this.minOffset = minOffset;
    this.maxOffset = maxOffset;
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  toDict(): Record<string, any> {
    return {
      minOffset: this.minOffset,
      maxOffset: this.maxOffset,
      lastUpdateTimestamp: this.lastUpdateTimestamp,
    };
  }

  static fromDict(d: Record<string, any>): TopicOffset {
    return new TopicOffset(
      d['minOffset'] != null ? d['minOffset'] : 0,
      d['maxOffset'] != null ? d['maxOffset'] : 0,
      d['lastUpdateTimestamp'] != null ? d['lastUpdateTimestamp'] : 0,
    );
  }

  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }

  static decode(data: Buffer): TopicOffset {
    return TopicOffset.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }

  toString(): string {
    return `TopicOffset[min=${this.minOffset}, max=${this.maxOffset}, ts=${this.lastUpdateTimestamp}]`;
  }
}

// ---------------------------------------------------------------- TopicStatsTable
export class TopicStatsTable {
  offsetTable = new Map<MessageQueue, TopicOffset>();
  topicPutTps = 0.0;

  toDict(): Record<string, any> {
    const offsetTable: Record<string, any> = {};
    for (const [k, v] of this.offsetTable) offsetTable[messageQueueKey(k)] = v.toDict();
    return {
      offsetTable,
      topicPutTps: this.topicPutTps,
    };
  }

  static fromDict(d: Record<string, any>): TopicStatsTable {
    const t = new TopicStatsTable();
    const decoded = decodeMessageQueueMap(d['offsetTable']);
    for (const [mq, v] of decoded) t.offsetTable.set(mq, TopicOffset.fromDict(v));
    t.topicPutTps = d['topicPutTps'] != null ? d['topicPutTps'] : 0.0;
    return t;
  }

  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }

  static decode(data: Buffer): TopicStatsTable {
    return TopicStatsTable.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------- ConsumeQueueData
export class ConsumeQueueData {
  physicOffset = 0;
  physicSize = 0;
  tagsCode = 0;
  extendDataJson: string | null = null;
  bitMap: string | null = null;
  eval = false;
  msg: string | null = null;

  constructor(
    physicOffset = 0,
    physicSize = 0,
    tagsCode = 0,
    extendDataJson: string | null = null,
    bitMap: string | null = null,
    eval_ = false,
    msg: string | null = null,
  ) {
    this.physicOffset = physicOffset;
    this.physicSize = physicSize;
    this.tagsCode = tagsCode;
    this.extendDataJson = extendDataJson;
    this.bitMap = bitMap;
    this.eval = eval_;
    this.msg = msg;
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = {
      physicOffset: this.physicOffset,
      physicSize: this.physicSize,
      tagsCode: this.tagsCode,
      eval: this.eval,
      bitMap: this.bitMap,
    };
    // null fields are not serialized
    if (this.extendDataJson != null) d['extendDataJson'] = this.extendDataJson;
    if (this.msg != null) d['msg'] = this.msg;
    return d;
  }

  static fromDict(d: Record<string, any>): ConsumeQueueData {
    return new ConsumeQueueData(
      d['physicOffset'] != null ? d['physicOffset'] : 0,
      d['physicSize'] != null ? d['physicSize'] : 0,
      d['tagsCode'] != null ? d['tagsCode'] : 0,
      d['extendDataJson'] != null ? d['extendDataJson'] : null,
      d['bitMap'] != null ? d['bitMap'] : null,
      d['eval'] != null ? d['eval'] : false,
      d['msg'] != null ? d['msg'] : null,
    );
  }
}

// ---------------------------------------------------------------- QueryConsumeQueueResponseBody
export class QueryConsumeQueueResponseBody {
  subscriptionData: Record<string, any> | null = null;
  filterData: string | null = null;
  queueData: ConsumeQueueData[] | null = null;
  maxQueueIndex = 0;
  minQueueIndex = 0;

  toDict(): Record<string, any> {
    const d: Record<string, any> = {
      maxQueueIndex: this.maxQueueIndex,
      minQueueIndex: this.minQueueIndex,
    };
    if (this.subscriptionData != null) d['subscriptionData'] = this.subscriptionData;
    if (this.filterData != null) d['filterData'] = this.filterData;
    if (this.queueData != null) d['queueData'] = this.queueData.map((q) => q.toDict());
    return d;
  }

  static fromDict(d: Record<string, any>): QueryConsumeQueueResponseBody {
    const b = new QueryConsumeQueueResponseBody();
    b.subscriptionData = d['subscriptionData'] != null ? d['subscriptionData'] : null;
    b.filterData = d['filterData'] != null ? d['filterData'] : null;
    const raw = d['queueData'];
    b.queueData = Array.isArray(raw) ? raw.map((x: Record<string, any>) => ConsumeQueueData.fromDict(x)) : null;
    b.maxQueueIndex = d['maxQueueIndex'] != null ? d['maxQueueIndex'] : 0;
    b.minQueueIndex = d['minQueueIndex'] != null ? d['minQueueIndex'] : 0;
    return b;
  }

  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }

  static decode(data: Buffer): QueryConsumeQueueResponseBody {
    return QueryConsumeQueueResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------- GroupForbidden
export class GroupForbidden {
  topic: string | null = null;
  group: string | null = null;
  readable: boolean | null = null;

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    if (this.topic != null) d['topic'] = this.topic;
    if (this.group != null) d['group'] = this.group;
    if (this.readable != null) d['readable'] = this.readable;
    return d;
  }

  static fromDict(d: Record<string, any>): GroupForbidden {
    const g = new GroupForbidden();
    g.topic = d['topic'] != null ? d['topic'] : null;
    g.group = d['group'] != null ? d['group'] : null;
    g.readable = d['readable'] != null ? !!d['readable'] : null;
    return g;
  }

  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }

  static decode(data: Buffer): GroupForbidden {
    return GroupForbidden.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------------------
// Auth user / ACL management bodies.
//
// Two rules decide the whole encoding:
//   * a field with no value is ABSENT from the JSON object, never present with
//     a null/empty value — the auth layer reads a present field as an explicit
//     change, so an unset one must not appear at all;
//   * the enum spellings are wire values and are case-exact. They are NOT
//     mutually consistent (`Super`/`Normal` vs `enable`/`disable`), so each is
//     written out verbatim rather than derived from a shared convention.
// ---------------------------------------------------------------------------
export const UserType = { SUPER: 'Super', NORMAL: 'Normal' };
export const UserStatus = { ENABLE: 'enable', DISABLE: 'disable' };
export const PolicyType = { CUSTOM: 'Custom', DEFAULT: 'Default' };
export const Decision = { ALLOW: 'Allow', DENY: 'Deny' };

function _put(d: Record<string, any>, key: string, value: any): void {
  if (value !== null && value !== undefined) d[key] = value;
}

export class UserInfo {
  username: string | null;
  password: string | null;
  userType: string | null;
  userStatus: string | null;

  constructor(username: string | null = null, password: string | null = null,
    userType: string | null = null, userStatus: string | null = null) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.userStatus = userStatus;
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'username', this.username);
    _put(d, 'password', this.password);
    _put(d, 'userType', this.userType);
    _put(d, 'userStatus', this.userStatus);
    return d;
  }

  static fromDict(d: Record<string, any>): UserInfo {
    return new UserInfo(
      d['username'] != null ? d['username'] : null,
      d['password'] != null ? d['password'] : null,
      d['userType'] != null ? d['userType'] : null,
      d['userStatus'] != null ? d['userStatus'] : null,
    );
  }

  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }

  static decode(data: Buffer): UserInfo {
    return UserInfo.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }

  static decodeList(data: Buffer): UserInfo[] {
    const arr = RemotingSerializable.decode(data);
    if (!Array.isArray(arr)) return [];
    return arr.map((d: Record<string, any>) => UserInfo.fromDict(d));
  }

  toString(): string {
    return `UserInfo[username=${this.username}, userType=${this.userType}, userStatus=${this.userStatus}]`;
  }
}

export class AclEntry {
  resource: string | null;
  actions: string[] | null;
  sourceIps: string[] | null;
  decision: string | null;

  constructor(resource: string | null = null, actions: string[] | null = null,
    sourceIps: string[] | null = null, decision: string | null = null) {
    this.resource = resource;
    this.actions = actions;
    this.sourceIps = sourceIps;
    this.decision = decision;
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'resource', this.resource);
    _put(d, 'actions', this.actions);
    _put(d, 'sourceIps', this.sourceIps);
    _put(d, 'decision', this.decision);
    return d;
  }

  static fromDict(d: Record<string, any>): AclEntry {
    return new AclEntry(
      d['resource'] != null ? d['resource'] : null,
      Array.isArray(d['actions']) ? d['actions'].map((a: any) => String(a)) : null,
      Array.isArray(d['sourceIps']) ? d['sourceIps'].map((s: any) => String(s)) : null,
      d['decision'] != null ? d['decision'] : null,
    );
  }
}

export class AclPolicy {
  policyType: string | null;
  entries: AclEntry[];

  constructor(policyType: string | null = null, entries: AclEntry[] = []) {
    this.policyType = policyType;
    this.entries = entries;
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'policyType', this.policyType);
    d['entries'] = this.entries.map((e) => e.toDict());
    return d;
  }

  static fromDict(d: Record<string, any>): AclPolicy {
    const raw = Array.isArray(d['entries']) ? d['entries'] : [];
    return new AclPolicy(
      d['policyType'] != null ? d['policyType'] : null,
      raw.map((e: Record<string, any>) => AclEntry.fromDict(e)),
    );
  }
}

export class AclInfo {
  subject: string | null;
  policies: AclPolicy[];

  constructor(subject: string | null = null, policies: AclPolicy[] = []) {
    this.subject = subject;
    this.policies = policies;
  }

  // One policy whose policyType stays unset, one entry per resource repeating
  // the same actions / sourceIps / decision.
  static fromResources(subject: string, resources: string[] | null, actions: string[] | null,
    sourceIps: string[] | null, decision: string | null): AclInfo {
    const entries = (resources != null ? resources : []).map(
      (r) => new AclEntry(r, actions, sourceIps, decision));
    return new AclInfo(subject, [new AclPolicy(null, entries)]);
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'subject', this.subject);
    d['policies'] = this.policies.map((p) => p.toDict());
    return d;
  }

  static fromDict(d: Record<string, any>): AclInfo {
    const raw = Array.isArray(d['policies']) ? d['policies'] : [];
    return new AclInfo(
      d['subject'] != null ? d['subject'] : null,
      raw.map((p: Record<string, any>) => AclPolicy.fromDict(p)),
    );
  }

  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }

  static decode(data: Buffer): AclInfo {
    return AclInfo.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }

  static decodeList(data: Buffer): AclInfo[] {
    const arr = RemotingSerializable.decode(data);
    if (!Array.isArray(arr)) return [];
    return arr.map((d: Record<string, any>) => AclInfo.fromDict(d));
  }

  toString(): string {
    return `AclInfo[subject=${this.subject}, policies=${this.policies.length}]`;
  }
}

// ---------------------------------------------------------------- Controller replica state
//
// The controller's replica-state answer: one ReplicasInfo per broker name, each
// carrying the master identity plus the in-sync / not-in-sync replica sets.
export class ReplicaIdentity {
  brokerName: string | null = null;
  brokerId: number | null = null;
  brokerAddress: string | null = null;
  alive: boolean | null = null;

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'brokerName', this.brokerName);
    _put(d, 'brokerId', this.brokerId);
    _put(d, 'brokerAddress', this.brokerAddress);
    _put(d, 'alive', this.alive);
    return d;
  }

  static fromDict(d: Record<string, any>): ReplicaIdentity {
    const r = new ReplicaIdentity();
    r.brokerName = d['brokerName'] != null ? String(d['brokerName']) : null;
    r.brokerId = d['brokerId'] != null ? parseInt(d['brokerId'], 10) : null;
    r.brokerAddress = d['brokerAddress'] != null ? String(d['brokerAddress']) : null;
    r.alive = d['alive'] != null ? !!d['alive'] : null;
    return r;
  }

  toString(): string {
    return `ReplicaIdentity[brokerName=${this.brokerName}, brokerId=${this.brokerId}, `
      + `brokerAddress=${this.brokerAddress}, alive=${this.alive}]`;
  }
}

export class ReplicasInfo {
  masterBrokerId: number | null = null;
  masterAddress: string | null = null;
  masterEpoch: number | null = null;
  syncStateSetEpoch: number | null = null;
  inSyncReplicas: ReplicaIdentity[] | null = null;
  notInSyncReplicas: ReplicaIdentity[] | null = null;

  toDict(): Record<string, any> {
    const d: Record<string, any> = {};
    _put(d, 'masterBrokerId', this.masterBrokerId);
    _put(d, 'masterAddress', this.masterAddress);
    _put(d, 'masterEpoch', this.masterEpoch);
    _put(d, 'syncStateSetEpoch', this.syncStateSetEpoch);
    if (this.inSyncReplicas != null) d['inSyncReplicas'] = this.inSyncReplicas.map((r) => r.toDict());
    if (this.notInSyncReplicas != null) d['notInSyncReplicas'] = this.notInSyncReplicas.map((r) => r.toDict());
    return d;
  }

  static fromDict(d: Record<string, any>): ReplicasInfo {
    const r = new ReplicasInfo();
    r.masterBrokerId = d['masterBrokerId'] != null ? parseInt(d['masterBrokerId'], 10) : null;
    r.masterAddress = d['masterAddress'] != null ? String(d['masterAddress']) : null;
    r.masterEpoch = d['masterEpoch'] != null ? parseInt(d['masterEpoch'], 10) : null;
    r.syncStateSetEpoch = d['syncStateSetEpoch'] != null ? parseInt(d['syncStateSetEpoch'], 10) : null;
    r.inSyncReplicas = Array.isArray(d['inSyncReplicas'])
      ? d['inSyncReplicas'].map((x: Record<string, any>) => ReplicaIdentity.fromDict(x)) : null;
    r.notInSyncReplicas = Array.isArray(d['notInSyncReplicas'])
      ? d['notInSyncReplicas'].map((x: Record<string, any>) => ReplicaIdentity.fromDict(x)) : null;
    return r;
  }
}

export class BrokerReplicasInfo {
  replicasInfoTable: Record<string, ReplicasInfo> = {};

  toDict(): Record<string, any> {
    const table: Record<string, any> = {};
    for (const [k, v] of Object.entries(this.replicasInfoTable)) table[k] = v.toDict();
    return { replicasInfoTable: table };
  }

  static fromDict(d: Record<string, any>): BrokerReplicasInfo {
    const b = new BrokerReplicasInfo();
    const raw = d['replicasInfoTable'];
    if (raw != null && typeof raw === 'object') {
      for (const [k, v] of Object.entries(raw as Record<string, any>)) {
        b.replicasInfoTable[k] = ReplicasInfo.fromDict(v as Record<string, any>);
      }
    }
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }

  static decode(data: Buffer): BrokerReplicasInfo {
    return BrokerReplicasInfo.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// CONTROLLER_GET_METADATA_INFO's answer rides the response extFields, where
// every value arrives as a string — "true"/"false" is the wire rendering of
// the boolean.
export class GetMetaDataResponseHeader {
  group: string | null = null;
  controllerLeaderId: string | null = null;
  controllerLeaderAddress: string | null = null;
  isLeader = false;
  peers: string | null = null;

  static fromDict(d: Record<string, any>): GetMetaDataResponseHeader {
    const h = new GetMetaDataResponseHeader();
    h.group = d['group'] != null ? String(d['group']) : null;
    h.controllerLeaderId = d['controllerLeaderId'] != null ? String(d['controllerLeaderId']) : null;
    h.controllerLeaderAddress = d['controllerLeaderAddress'] != null ? String(d['controllerLeaderAddress']) : null;
    h.isLeader = d['isLeader'] === true || d['isLeader'] === 'true';
    h.peers = d['peers'] != null ? String(d['peers']) : null;
    return h;
  }

  toString(): string {
    return `GetMetaDataResponseHeader[group=${this.group}, leaderId=${this.controllerLeaderId}, `
      + `leaderAddress=${this.controllerLeaderAddress}, isLeader=${this.isLeader}]`;
  }
}

// ---------------------------------------------------------------- RocksDB consume-queue check
export const CheckRocksdbCqWriteStatus = {
  CHECK_OK: 0,
  CHECK_NOT_OK: 1,
  CHECK_IN_PROGRESS: 2,
  CHECK_ERROR: 3,
} as const;

export class CheckRocksdbCqWriteResult {
  checkResult: string | null = null;
  checkStatus: number = CheckRocksdbCqWriteStatus.CHECK_OK;

  toDict(): Record<string, any> {
    const d: Record<string, any> = { checkStatus: this.checkStatus };
    _put(d, 'checkResult', this.checkResult);
    return d;
  }

  static fromDict(d: Record<string, any>): CheckRocksdbCqWriteResult {
    const r = new CheckRocksdbCqWriteResult();
    r.checkResult = d['checkResult'] != null ? String(d['checkResult']) : null;
    r.checkStatus = d['checkStatus'] != null ? parseInt(d['checkStatus'], 10) : CheckRocksdbCqWriteStatus.CHECK_OK;
    return r;
  }

  encode(): Buffer { return RemotingSerializable.encode(this.toDict()); }

  static decode(data: Buffer): CheckRocksdbCqWriteResult {
    return CheckRocksdbCqWriteResult.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }

  toString(): string {
    return `CheckRocksdbCqWriteResult[checkResult=${this.checkResult}, checkStatus=${this.checkStatus}]`;
  }
}

// ---------------------------------------------------------------- Lite (LMQ) admin bodies
export class GetBrokerLiteInfoResponseBody {
  storeType: string | null = null;
  maxLmqNum = 0;
  currentLmqNum = 0;
  liteSubscriptionCount = 0;
  orderInfoCount = 0;
  cqTableSize = 0;
  offsetTableSize = 0;
  eventMapSize = 0;
  topicMeta: Record<string, number> | null = null;
  groupMeta: Record<string, string[]> | null = null;

  static fromDict(d: Record<string, any>): GetBrokerLiteInfoResponseBody {
    const b = new GetBrokerLiteInfoResponseBody();
    b.storeType = d['storeType'] != null ? String(d['storeType']) : null;
    b.maxLmqNum = d['maxLmqNum'] != null ? parseInt(d['maxLmqNum'], 10) : 0;
    b.currentLmqNum = d['currentLmqNum'] != null ? parseInt(d['currentLmqNum'], 10) : 0;
    b.liteSubscriptionCount = d['liteSubscriptionCount'] != null ? parseInt(d['liteSubscriptionCount'], 10) : 0;
    b.orderInfoCount = d['orderInfoCount'] != null ? parseInt(d['orderInfoCount'], 10) : 0;
    b.cqTableSize = d['cqTableSize'] != null ? parseInt(d['cqTableSize'], 10) : 0;
    b.offsetTableSize = d['offsetTableSize'] != null ? parseInt(d['offsetTableSize'], 10) : 0;
    b.eventMapSize = d['eventMapSize'] != null ? parseInt(d['eventMapSize'], 10) : 0;
    b.topicMeta = d['topicMeta'] != null && typeof d['topicMeta'] === 'object' ? d['topicMeta'] : null;
    b.groupMeta = d['groupMeta'] != null && typeof d['groupMeta'] === 'object' ? d['groupMeta'] : null;
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this as unknown as Record<string, any>); }

  static decode(data: Buffer): GetBrokerLiteInfoResponseBody {
    return GetBrokerLiteInfoResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class GetParentTopicInfoResponseBody {
  topic: string | null = null;
  ttl = 0;
  groups: string[] | null = null;
  lmqNum = 0;
  liteTopicCount = 0;

  static fromDict(d: Record<string, any>): GetParentTopicInfoResponseBody {
    const b = new GetParentTopicInfoResponseBody();
    b.topic = d['topic'] != null ? String(d['topic']) : null;
    b.ttl = d['ttl'] != null ? parseInt(d['ttl'], 10) : 0;
    b.groups = Array.isArray(d['groups']) ? d['groups'].map((g: any) => String(g)) : null;
    b.lmqNum = d['lmqNum'] != null ? parseInt(d['lmqNum'], 10) : 0;
    b.liteTopicCount = d['liteTopicCount'] != null ? parseInt(d['liteTopicCount'], 10) : 0;
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this as unknown as Record<string, any>); }

  static decode(data: Buffer): GetParentTopicInfoResponseBody {
    return GetParentTopicInfoResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class GetLiteTopicInfoResponseBody {
  parentTopic: string | null = null;
  liteTopic: string | null = null;
  subscriber: Record<string, any>[] | null = null;
  topicOffset: Record<string, any> | null = null;
  shardingToBroker = false;

  static fromDict(d: Record<string, any>): GetLiteTopicInfoResponseBody {
    const b = new GetLiteTopicInfoResponseBody();
    b.parentTopic = d['parentTopic'] != null ? String(d['parentTopic']) : null;
    b.liteTopic = d['liteTopic'] != null ? String(d['liteTopic']) : null;
    b.subscriber = Array.isArray(d['subscriber']) ? d['subscriber'] : null;
    b.topicOffset = d['topicOffset'] != null && typeof d['topicOffset'] === 'object' ? d['topicOffset'] : null;
    b.shardingToBroker = d['shardingToBroker'] != null ? !!d['shardingToBroker'] : false;
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this as unknown as Record<string, any>); }

  static decode(data: Buffer): GetLiteTopicInfoResponseBody {
    return GetLiteTopicInfoResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class GetLiteClientInfoResponseBody {
  parentTopic: string | null = null;
  group: string | null = null;
  clientId: string | null = null;
  lastAccessTime = 0;
  lastConsumeTime = 0;
  liteTopicCount = 0;
  liteTopicSet: string[] | null = null;

  static fromDict(d: Record<string, any>): GetLiteClientInfoResponseBody {
    const b = new GetLiteClientInfoResponseBody();
    b.parentTopic = d['parentTopic'] != null ? String(d['parentTopic']) : null;
    b.group = d['group'] != null ? String(d['group']) : null;
    b.clientId = d['clientId'] != null ? String(d['clientId']) : null;
    b.lastAccessTime = d['lastAccessTime'] != null ? parseInt(d['lastAccessTime'], 10) : 0;
    b.lastConsumeTime = d['lastConsumeTime'] != null ? parseInt(d['lastConsumeTime'], 10) : 0;
    b.liteTopicCount = d['liteTopicCount'] != null ? parseInt(d['liteTopicCount'], 10) : 0;
    b.liteTopicSet = Array.isArray(d['liteTopicSet']) ? d['liteTopicSet'].map((t: any) => String(t)) : null;
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this as unknown as Record<string, any>); }

  static decode(data: Buffer): GetLiteClientInfoResponseBody {
    return GetLiteClientInfoResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class LiteLagInfo {
  liteTopic: string | null = null;
  lagCount = 0;
  earliestUnconsumedTimestamp = -1;
  consumerOffset = -1;

  static fromDict(d: Record<string, any>): LiteLagInfo {
    const l = new LiteLagInfo();
    l.liteTopic = d['liteTopic'] != null ? String(d['liteTopic']) : null;
    l.lagCount = d['lagCount'] != null ? parseInt(d['lagCount'], 10) : 0;
    l.earliestUnconsumedTimestamp = d['earliestUnconsumedTimestamp'] != null
      ? parseInt(d['earliestUnconsumedTimestamp'], 10) : -1;
    l.consumerOffset = d['consumerOffset'] != null ? parseInt(d['consumerOffset'], 10) : -1;
    return l;
  }
}

export class GetLiteGroupInfoResponseBody {
  group: string | null = null;
  parentTopic: string | null = null;
  liteTopic: string | null = null;
  earliestUnconsumedTimestamp = -1;
  totalLagCount = 0;
  liteTopicOffsetWrapper: Record<string, any> | null = null;
  lagCountTopK: LiteLagInfo[] | null = null;
  lagTimestampTopK: LiteLagInfo[] | null = null;

  static fromDict(d: Record<string, any>): GetLiteGroupInfoResponseBody {
    const b = new GetLiteGroupInfoResponseBody();
    b.group = d['group'] != null ? String(d['group']) : null;
    b.parentTopic = d['parentTopic'] != null ? String(d['parentTopic']) : null;
    b.liteTopic = d['liteTopic'] != null ? String(d['liteTopic']) : null;
    b.earliestUnconsumedTimestamp = d['earliestUnconsumedTimestamp'] != null
      ? parseInt(d['earliestUnconsumedTimestamp'], 10) : -1;
    b.totalLagCount = d['totalLagCount'] != null ? parseInt(d['totalLagCount'], 10) : 0;
    b.liteTopicOffsetWrapper = d['liteTopicOffsetWrapper'] != null
      && typeof d['liteTopicOffsetWrapper'] === 'object' ? d['liteTopicOffsetWrapper'] : null;
    b.lagCountTopK = Array.isArray(d['lagCountTopK'])
      ? d['lagCountTopK'].map((x: Record<string, any>) => LiteLagInfo.fromDict(x)) : null;
    b.lagTimestampTopK = Array.isArray(d['lagTimestampTopK'])
      ? d['lagTimestampTopK'].map((x: Record<string, any>) => LiteLagInfo.fromDict(x)) : null;
    return b;
  }

  encode(): Buffer { return RemotingSerializable.encode(this as unknown as Record<string, any>); }

  static decode(data: Buffer): GetLiteGroupInfoResponseBody {
    return GetLiteGroupInfoResponseBody.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------- Concurrent admin tooling results
export class BrokerOperatorResult {
  successList: string[] = [];
  failureList: string[] = [];

  toDict(): Record<string, any> {
    return { successList: this.successList.slice(), failureList: this.failureList.slice() };
  }

  static fromDict(d: Record<string, any>): BrokerOperatorResult {
    const r = new BrokerOperatorResult();
    r.successList = Array.isArray(d['successList']) ? d['successList'].map((s: any) => String(s)) : [];
    r.failureList = Array.isArray(d['failureList']) ? d['failureList'].map((s: any) => String(s)) : [];
    return r;
  }

  toString(): string {
    return `BrokerOperatorResult{successList=${JSON.stringify(this.successList)}, `
      + `failureList=${JSON.stringify(this.failureList)}}`;
  }
}

export const AdminToolsResultCodeEnum = {
  SUCCESS: 200,
  REMOTING_ERROR: -1001,
  MQ_BROKER_ERROR: -1002,
  MQ_CLIENT_ERROR: -1003,
  INTERRUPT_ERROR: -1004,
  TOPIC_ROUTE_INFO_NOT_EXIST: -2001,
  CONSUMER_NOT_ONLINE: -2002,
  BROADCAST_CONSUMPTION: -2003,
} as const;

export class AdminToolResult<T = any> {
  success: boolean;
  code: number;
  errorMsg: string | null;
  data: T | null;

  constructor(success: boolean, code: number, errorMsg: string | null = null, data: T | null = null) {
    this.success = success;
    this.code = code;
    this.errorMsg = errorMsg;
    this.data = data;
  }

  static success<T>(data: T): AdminToolResult<T> {
    return new AdminToolResult<T>(true, AdminToolsResultCodeEnum.SUCCESS, 'success', data);
  }

  static failure(code: number, errorMsg: string | null, data: any = null): AdminToolResult<any> {
    return new AdminToolResult<any>(false, code, errorMsg, data);
  }

  toDict(): Record<string, any> {
    const d: Record<string, any> = { success: this.success, code: this.code };
    if (this.errorMsg != null) d['errorMsg'] = this.errorMsg;
    if (this.data != null) d['data'] = this.data;
    return d;
  }

  toString(): string {
    return `AdminToolResult{success=${this.success}, code=${this.code}, `
      + `errorMsg=${this.errorMsg}, data=${this.data != null ? JSON.stringify(this.data.toDict ? this.data.toDict() : this.data) : 'null'}}`;
  }
}

export const __all__ = [
  'messageQueueKey',
  'parseMessageQueueKey',
  'decodeMessageQueueMap',
  'TopicConfig',
  'TopicConfigSerializeWrapper',
  'OffsetWrapper',
  'ConsumeStats',
  'ConsumerRunningInfo',
  'TopicOffset',
  'TopicStatsTable',
  'ConsumeQueueData',
  'QueryConsumeQueueResponseBody',
  'GroupForbidden',
  'UserType',
  'UserStatus',
  'PolicyType',
  'Decision',
  'UserInfo',
  'AclEntry',
  'AclPolicy',
  'AclInfo',
  'ReplicaIdentity',
  'ReplicasInfo',
  'BrokerReplicasInfo',
  'GetMetaDataResponseHeader',
  'CheckRocksdbCqWriteStatus',
  'CheckRocksdbCqWriteResult',
  'GetBrokerLiteInfoResponseBody',
  'GetParentTopicInfoResponseBody',
  'GetLiteTopicInfoResponseBody',
  'GetLiteClientInfoResponseBody',
  'LiteLagInfo',
  'GetLiteGroupInfoResponseBody',
  'BrokerOperatorResult',
  'AdminToolsResultCodeEnum',
  'AdminToolResult',
];
