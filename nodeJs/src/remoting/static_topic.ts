// -*- coding: utf-8 -*-
// Static topic (unitized topic) mapping support.
import { MessageQueue } from '../common/message.ts';
import { RemotingSerializable } from './serialize.ts';
// type-only: route.ts imports the runtime helpers below, a value import here
// would be a circular runtime dependency.
import type { TopicRouteData } from './route.ts';

// MixAll.METADATA_SCOPE_GLOBAL — the scope every route-side mapping info uses
// unless a migration explicitly scoped it otherwise.
export const METADATA_SCOPE_GLOBAL = '__global__';
// MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX — logical MessageQueues carry a
// broker name of this prefix + scope; the real broker is resolved through the
// topicEndPointsTable.
export const LOGICAL_QUEUE_MOCK_BROKER_PREFIX = '__syslo__';
// MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST — a logical queue whose
// mapping info lists no hosting broker maps to this sentinel.
export const LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST = '__syslo__none__';

export function isLogicalQueueMockBrokerName(brokerName: string): boolean {
  return typeof brokerName === 'string' && brokerName.startsWith(LOGICAL_QUEUE_MOCK_BROKER_PREFIX);
}

// TopicQueueMappingUtils.getMockBrokerName: "__global__" (the scope) loses its
// first two chars, every other scope is prefixed verbatim.
export function getMockBrokerName(scope: string): string {
  if (scope === METADATA_SCOPE_GLOBAL) {
    return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope.substring(2);
  }
  return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope;
}

// ---------------------------------------------------------------------------
// LogicQueueMappingItem — one generation of a logical queue on one broker.
// ---------------------------------------------------------------------------

export class LogicQueueMappingItem {
  gen = 0;            // immutable generation
  queueId = 0;        // immutable physical queue id on bname
  bname = '';         // immutable hosting broker name
  logicOffset = 0;    // start of the logic offset; changed by command once
  startOffset = 0;    // start of the physical offset, always 0, immutable
  endOffset = -1;     // end of the physical offset, excluded, -1 = open
  timeOfStart = -1;   // mutable, reserved
  timeOfEnd = -1;     // mutable, reserved

  constructor(
    gen = 0, queueId = 0, bname = '',
    logicOffset = 0, startOffset = 0, endOffset = -1,
    timeOfStart = -1, timeOfEnd = -1,
  ) {
    this.gen = gen;
    this.queueId = queueId;
    this.bname = bname;
    this.logicOffset = logicOffset;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.timeOfStart = timeOfStart;
    this.timeOfEnd = timeOfEnd;
  }

  // The last item of a hostedQueues list is the CURRENT generation.
  isBrokered(): boolean {
    return this.bname !== '';
  }

  toDict(): Record<string, any> {
    return {
      gen: this.gen,
      queueId: this.queueId,
      bname: this.bname,
      logicOffset: this.logicOffset,
      startOffset: this.startOffset,
      endOffset: this.endOffset,
      timeOfStart: this.timeOfStart,
      timeOfEnd: this.timeOfEnd,
    };
  }

  static fromDict(d: Record<string, any> | null | undefined): LogicQueueMappingItem {
    d = d || {};
    return new LogicQueueMappingItem(
      d['gen'] != null ? d['gen'] : 0,
      d['queueId'] != null ? d['queueId'] : 0,
      d['bname'] != null ? d['bname'] : '',
      d['logicOffset'] != null ? d['logicOffset'] : 0,
      d['startOffset'] != null ? d['startOffset'] : 0,
      d['endOffset'] != null ? d['endOffset'] : -1,
      d['timeOfStart'] != null ? d['timeOfStart'] : -1,
      d['timeOfEnd'] != null ? d['timeOfEnd'] : -1,
    );
  }
}

// ---------------------------------------------------------------------------
// TopicQueueMappingInfo — the per-broker view carried INSIDE the route.
// ---------------------------------------------------------------------------

export class TopicQueueMappingInfo {
  topic = '';
  scope = METADATA_SCOPE_GLOBAL;
  totalQueues = 0;
  bname = '';
  epoch = 0;
  dirty = false;
  // logicId -> physicalId, JSON keys are stringified ints.
  currIdMap: Record<string, number> = {};

  constructor(
    topic = '', totalQueues = 0, bname = '', epoch = 0,
    scope = METADATA_SCOPE_GLOBAL, dirty = false,
    currIdMap: Record<string, number> | null = null,
  ) {
    this.topic = topic;
    this.totalQueues = totalQueues;
    this.bname = bname;
    this.epoch = epoch;
    this.scope = scope;
    this.dirty = dirty;
    this.currIdMap = currIdMap != null ? currIdMap : {};
  }

  toDict(): Record<string, any> {
    const curr: Record<string, number> = {};
    for (const [k, v] of Object.entries(this.currIdMap)) curr[String(k)] = v;
    return {
      topic: this.topic,
      scope: this.scope,
      totalQueues: this.totalQueues,
      bname: this.bname,
      epoch: this.epoch,
      dirty: this.dirty,
      currIdMap: curr,
    };
  }

  static fromDict(d: Record<string, any> | null | undefined): TopicQueueMappingInfo {
    d = d || {};
    const curr: Record<string, number> = {};
    const rawCurr = d['currIdMap'] || {};
    for (const [k, v] of Object.entries(rawCurr)) curr[String(k)] = Number(v);
    return new TopicQueueMappingInfo(
      d['topic'] != null ? d['topic'] : '',
      d['totalQueues'] != null ? d['totalQueues'] : 0,
      d['bname'] != null ? d['bname'] : '',
      d['epoch'] != null ? d['epoch'] : 0,
      d['scope'] != null ? d['scope'] : METADATA_SCOPE_GLOBAL,
      d['dirty'] === true,
      curr,
    );
  }
}

// ---------------------------------------------------------------------------
// TopicQueueMappingDetail — the full mapping document a static-topic CREATE
// carries in its body (one broker's shard of it).
// ---------------------------------------------------------------------------

export class TopicQueueMappingDetail {
  topic = '';
  scope = METADATA_SCOPE_GLOBAL;
  totalQueues = 0;
  bname = '';
  epoch = 0;
  dirty = false;
  // globalId -> physicalId on THIS broker, JSON keys are stringified ints.
  currIdMap: Record<string, number> = {};
  // globalId -> generation list; JSON keys are stringified ints.
  hostedQueues: Record<string, LogicQueueMappingItem[]> = {};

  constructor(
    topic = '', totalQueues = 0, bname = '', epoch = 0,
    scope = METADATA_SCOPE_GLOBAL, dirty = false,
    currIdMap: Record<string, number> | null = null,
    hostedQueues: Record<string, LogicQueueMappingItem[]> | null = null,
  ) {
    this.topic = topic;
    this.totalQueues = totalQueues;
    this.bname = bname;
    this.epoch = epoch;
    this.scope = scope;
    this.dirty = dirty;
    this.currIdMap = currIdMap != null ? currIdMap : {};
    this.hostedQueues = hostedQueues != null ? hostedQueues : {};
  }

  toDict(): Record<string, any> {
    const curr: Record<string, number> = {};
    for (const [k, v] of Object.entries(this.currIdMap)) curr[String(k)] = v;
    const hosted: Record<string, any[]> = {};
    for (const [globalId, items] of Object.entries(this.hostedQueues)) {
      hosted[String(globalId)] = items.map((it) => it.toDict());
    }
    return {
      topic: this.topic,
      scope: this.scope,
      totalQueues: this.totalQueues,
      bname: this.bname,
      epoch: this.epoch,
      dirty: this.dirty,
      currIdMap: curr,
      hostedQueues: hosted,
    };
  }

  encode(): Buffer {
    const data = RemotingSerializable.encode(this.toDict());
    return data != null ? data : Buffer.alloc(0);
  }

  static fromDict(d: Record<string, any> | null | undefined): TopicQueueMappingDetail {
    d = d || {};
    const curr: Record<string, number> = {};
    for (const [k, v] of Object.entries(d['currIdMap'] || {})) curr[String(k)] = Number(v);
    const hosted: Record<string, LogicQueueMappingItem[]> = {};
    for (const [globalId, items] of Object.entries(d['hostedQueues'] || {})) {
      hosted[String(globalId)] = (items as any[]).map((it) => LogicQueueMappingItem.fromDict(it));
    }
    return new TopicQueueMappingDetail(
      d['topic'] != null ? d['topic'] : '',
      d['totalQueues'] != null ? d['totalQueues'] : 0,
      d['bname'] != null ? d['bname'] : '',
      d['epoch'] != null ? d['epoch'] : 0,
      d['scope'] != null ? d['scope'] : METADATA_SCOPE_GLOBAL,
      d['dirty'] === true,
      curr,
      hosted,
    );
  }
}

// ---------------------------------------------------------------------------
// topicRouteData2EndpointsForStaticTopic — the pure routing core.
// ---------------------------------------------------------------------------

// Port of ClientMetadata.topicRouteData2EndpointsForStaticTopic: from the
// route's topicQueueMappingByBroker (brokerName -> TopicQueueMappingInfo JSON)
// build the LOGICAL queue set — MessageQueue(topic, mockBrokerName(scope),
// globalId) -> the REAL broker name hosting it ("__syslo__none__" when the
// logical queue exists in the totals but no broker hosts it yet).
export function topicRouteData2EndpointsForStaticTopic(
  topic: string, route: TopicRouteData,
): Map<MessageQueue, string> {
  const endpointsOfBroker = new Map<MessageQueue, string>();
  const mappingByBroker = route.topicQueueMappingByBroker;
  if (mappingByBroker == null || Object.keys(mappingByBroker).length === 0) {
    return endpointsOfBroker;
  }

  const infosByScope = new Map<string, Map<string, TopicQueueMappingInfo>>();
  for (const [brokerName, raw] of Object.entries(mappingByBroker)) {
    const info = TopicQueueMappingInfo.fromDict(raw as Record<string, any>);
    if (info.scope != null) {
      let byScope = infosByScope.get(info.scope);
      if (byScope == null) {
        byScope = new Map<string, TopicQueueMappingInfo>();
        infosByScope.set(info.scope, byScope);
      }
      byScope.set(brokerName, info);
    }
  }

  for (const [scope, infoMap] of infosByScope.entries()) {
    // epoch-desc; Array.prototype.sort is stable (spec), matching
    // Collections.sort's tie behaviour.
    const mappingInfos = Array.from(infoMap.entries())
      .sort(([, a], [, b]) => b.epoch - a.epoch);
    let maxTotalNums = 0;
    const maxTotalNumOfEpoch = -1;
    const mqKey = (mq: MessageQueue): string => `${mq.topic}@${mq.brokerName}@${mq.queueId}`;
    const mqEndPoints = new Map<string, TopicQueueMappingInfo>();
    for (const [, info] of mappingInfos) {
      if (info.epoch >= maxTotalNumOfEpoch && info.totalQueues > maxTotalNums) {
        maxTotalNums = info.totalQueues;
      }
      for (const [logicIdStr, physicalId] of Object.entries(info.currIdMap)) {
        const globalId = parseInt(logicIdStr, 10);
        const mq = new MessageQueue(topic, getMockBrokerName(scope), globalId);
        const oldInfo = mqEndPoints.get(mqKey(mq));
        if (oldInfo == null || oldInfo.epoch <= info.epoch) {
          mqEndPoints.set(mqKey(mq), info);
        }
      }
    }

    // Accomplish the static logic queues: every id below the total exists;
    // unhosted ones point at the NOT_EXIST sentinel.
    for (let i = 0; i < maxTotalNums; i++) {
      const mq = new MessageQueue(topic, getMockBrokerName(scope), i);
      const holder = mqEndPoints.get(mqKey(mq));
      if (holder == null) {
        endpointsOfBroker.set(mq, LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
      } else {
        endpointsOfBroker.set(mq, holder.bname);
      }
    }
  }
  return endpointsOfBroker;
}

// Map<MessageQueue, string> keyed by MessageQueue OBJECT — expose a plain
// [key-string -> realBrokerName] view for tests and logging.
export function endpointsToKeyStrings(
  endpoints: Map<MessageQueue, string>,
): Map<string, string> {
  const out = new Map<string, string>();
  for (const [mq, real] of endpoints.entries()) {
    out.set(`${mq.topic}@${mq.brokerName}@${mq.queueId}`, real);
  }
  return out;
}
