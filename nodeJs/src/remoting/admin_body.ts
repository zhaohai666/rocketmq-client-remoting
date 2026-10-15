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
];
