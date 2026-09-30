// -*- coding: utf-8 -*-
/**
 * 管理端专用响应体（对应 org.apache.rocketmq.remoting.protocol.admin.* 与 body.* 中的管理类）：
 *
 * TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
 * TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody。
 *
 * ⚠ 关键坑：这几个类的 Map 键是 **MessageQueue**，fastjson2 会把键直接内联成 JSON 对象，
 * 产出**非法 JSON**，例如：
 *     {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
 * 所以必须用 RemotingSerializable.decode（fastjson 容错解析），再用 messageQueueKey /
 * decodeMessageQueueMap 把字符串键还原成 MessageQueue。
 * 字段名与结构均由 Java 探针实测确认。
 *
 * 本模块复用 ./bodies.ts 中已实现的共享管理体（OffsetWrapper / ConsumeStats /
 * TopicConfigSerializeWrapper / TopicConfig / ConsumerRunningInfo 及其 MessageQueue 键助手），
 * 仅补充 bodies.ts 尚未覆盖的 TopicOffset / TopicStatsTable / ConsumeQueueData /
 * QueryConsumeQueueResponseBody。
 */
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

// Re-export 共享管理体，使单一 import 即可覆盖所有 admin body。
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
  /** 对应 org.apache.rocketmq.remoting.protocol.admin.TopicOffset。 */
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
  /** 对应 org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable。
   *
   * 探针输出：{"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}
   */
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
  /** 对应 org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData。
   *
   * 探针/源码字段：physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg
   */
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
    // 同 Java：null 字段不序列化
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
  /** 对应 org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody。
   *
   * 探针输出：{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}
   * （queueData 为 null 时不出现）
   */
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
