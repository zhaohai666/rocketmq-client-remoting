// -*- coding: utf-8 -*-
// Heartbeat data (org.apache.rocketmq.remoting.protocol.heartbeat.*).
// Mirrors python/rocketmq/remoting/protocol/heartbeat.py.
//
// NOTE: the heartbeat SubscriptionData uses the SAME fields as
// common/subscriptionData.ts (topic, subString, tagsSet, codeSet, subVersion,
// expressionType, classFilterMode) and the same JSON keys, so we reuse it here.
// The heartbeat ConsumerData deliberately does NOT include consumeTimestamp /
// maxReconsumeTimes (those were 4.x-only); only 5.x fields are emitted.
import { SubscriptionData } from '../common/subscriptionData.ts';
import { RemotingSerializable } from './serialize.ts';

export const ConsumeType = {
  CONSUME_ACTIVELY: 'CONSUME_ACTIVELY',
  CONSUME_PASSIVELY: 'CONSUME_PASSIVELY',
};

export const MessageModel = {
  CLUSTERING: 'CLUSTERING',
  BROADCASTING: 'BROADCASTING',
};

export const ConsumeFromWhere = {
  CONSUME_FROM_LAST_OFFSET: 'CONSUME_FROM_LAST_OFFSET',
  CONSUME_FROM_FIRST_OFFSET: 'CONSUME_FROM_FIRST_OFFSET',
  CONSUME_FROM_TIMESTAMP: 'CONSUME_FROM_TIMESTAMP',
};

export class ProducerData {
  groupName: string;
  enable: boolean;
  constructor(groupName = '') {
    this.groupName = groupName;
    this.enable = true;
  }
  toDict(): Record<string, any> {
    return { groupName: this.groupName, enable: this.enable };
  }
  static fromDict(d: Record<string, any>): ProducerData {
    const p = new ProducerData(d['groupName'] != null ? d['groupName'] : '');
    if (d['enable'] === false) p.enable = false;
    return p;
  }
}

export class ConsumerData {
  groupName: string;
  consumeType: string;
  messageModel: string;
  consumeFromWhere: string;
  subscriptionDataSet: SubscriptionData[];
  unitMode: boolean;
  constructor(
    groupName = '',
    consumeType = ConsumeType.CONSUME_PASSIVELY,
    messageModel = MessageModel.CLUSTERING,
    consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
  ) {
    this.groupName = groupName;
    this.consumeType = consumeType;
    this.messageModel = messageModel;
    this.consumeFromWhere = consumeFromWhere;
    this.subscriptionDataSet = [];
    this.unitMode = false;
  }
  toDict(): Record<string, any> {
    return {
      groupName: this.groupName,
      consumeType: this.consumeType,
      messageModel: this.messageModel,
      consumeFromWhere: this.consumeFromWhere,
      subscriptionDataSet: this.subscriptionDataSet.map((s) => s.toDict()),
      unitMode: this.unitMode,
    };
  }
  static fromDict(d: Record<string, any>): ConsumerData {
    const cd = new ConsumerData(
      d['groupName'] != null ? d['groupName'] : '',
      d['consumeType'] != null ? d['consumeType'] : ConsumeType.CONSUME_PASSIVELY,
      d['messageModel'] != null ? d['messageModel'] : MessageModel.CLUSTERING,
      d['consumeFromWhere'] != null ? d['consumeFromWhere'] : ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
    );
    cd.unitMode = d['unitMode'] === true;
    const set = d['subscriptionDataSet'] || [];
    for (const sd of set) {
      const sub = new SubscriptionData(sd['topic'], sd['subString']);
      sub.subVersion = parseInt(sd['subVersion'] != null ? sd['subVersion'] : 0, 10);
      sub.expressionType = sd['expressionType'] != null ? sd['expressionType'] : 'TAG';
      sub.classFilterMode = sd['classFilterMode'] === true;
      sub.tagsSet = new Set((sd['tagsSet'] || []).slice());
      sub.codeSet = new Set((sd['codeSet'] || []).map((v: any) => parseInt(v, 10)));
      cd.subscriptionDataSet.push(sub);
    }
    return cd;
  }
}

export class HeartbeatData {
  clientID: string;
  producerDataSet: ProducerData[];
  consumerDataSet: ConsumerData[];
  withoutSub: boolean;
  constructor(clientID = '') {
    this.clientID = clientID;
    this.producerDataSet = [];
    this.consumerDataSet = [];
    this.withoutSub = false;
  }
  toDict(): Record<string, any> {
    // heartbeatFingerprint intentionally 0 (broker takes the V1 register path);
    // withoutSub mirrors Java's isWithoutSub / fastjson2 name withoutSub.
    return {
      clientID: this.clientID,
      producerDataSet: this.producerDataSet.map((p) => p.toDict()),
      consumerDataSet: this.consumerDataSet.map((c) => c.toDict()),
      heartbeatFingerprint: 0,
      withoutSub: this.withoutSub,
    };
  }
  static fromDict(d: Record<string, any>): HeartbeatData {
    const hb = new HeartbeatData(d['clientID'] != null ? d['clientID'] : '');
    for (const p of d['producerDataSet'] || []) hb.producerDataSet.push(ProducerData.fromDict(p));
    for (const c of d['consumerDataSet'] || []) hb.consumerDataSet.push(ConsumerData.fromDict(c));
    hb.withoutSub = d['withoutSub'] === true;
    return hb;
  }
  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }
  static decode(data: Buffer): HeartbeatData {
    return HeartbeatData.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export function heartbeatDataEncode(hd: HeartbeatData): Buffer {
  return RemotingSerializable.encode(hd.toDict());
}

export function heartbeatDataDecode(bs: Buffer): HeartbeatData {
  return HeartbeatData.fromDict(RemotingSerializable.decode(bs) as Record<string, any>);
}

export default { HeartbeatData, ConsumerData, ProducerData, heartbeatDataEncode, heartbeatDataDecode, ConsumeType, MessageModel, ConsumeFromWhere };
