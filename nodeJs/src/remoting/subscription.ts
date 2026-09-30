// -*- coding: utf-8 -*-
// Subscription-group models (org.apache.rocketmq.remoting.protocol.subscription.*).
// Mirrors python/rocketmq/remoting/protocol/subscription.py.
// Field names and defaults are taken from a Java 5.x probe of
// JSON.toJSONString(new SubscriptionGroupConfig()).
import { RemotingSerializable } from './serialize.ts';

const MASTER_ID = 0;

export const GroupRetryPolicyType = {
  EXPONENTIAL: 'EXPONENTIAL',
  CUSTOMIZED: 'CUSTOMIZED',
};

export class GroupRetryPolicy {
  type: string;
  exponentialRetryPolicy: Record<string, any> | null;
  customizedRetryPolicy: Record<string, any> | null;
  constructor(
    policyType = GroupRetryPolicyType.CUSTOMIZED,
    exponentialRetryPolicy: Record<string, any> | null = null,
    customizedRetryPolicy: Record<string, any> | null = null,
  ) {
    this.type = policyType;
    this.exponentialRetryPolicy = exponentialRetryPolicy;
    this.customizedRetryPolicy = customizedRetryPolicy;
  }
  toDict(): Record<string, any> {
    const d: Record<string, any> = { type: this.type };
    if (this.exponentialRetryPolicy != null) d['exponentialRetryPolicy'] = this.exponentialRetryPolicy;
    if (this.customizedRetryPolicy != null) d['customizedRetryPolicy'] = this.customizedRetryPolicy;
    return d;
  }
  static fromDict(d: Record<string, any> | null | undefined): GroupRetryPolicy {
    const dd = d || {};
    return new GroupRetryPolicy(
      dd['type'] != null ? dd['type'] : GroupRetryPolicyType.CUSTOMIZED,
      dd['exponentialRetryPolicy'] != null ? dd['exponentialRetryPolicy'] : null,
      dd['customizedRetryPolicy'] != null ? dd['customizedRetryPolicy'] : null,
    );
  }
}

export class SimpleSubscriptionData {
  topic: string;
  expressionType: string;
  expression: string;
  version: number;
  constructor(topic = '', expressionType = 'TAG', expression = '*', version = 0) {
    this.topic = topic;
    this.expressionType = expressionType;
    this.expression = expression;
    this.version = version;
  }
  toDict(): Record<string, any> {
    return {
      topic: this.topic,
      expressionType: this.expressionType,
      expression: this.expression,
      version: this.version,
    };
  }
  static fromDict(d: Record<string, any>): SimpleSubscriptionData {
    return new SimpleSubscriptionData(
      d['topic'] != null ? d['topic'] : '',
      d['expressionType'] != null ? d['expressionType'] : 'TAG',
      d['expression'] != null ? d['expression'] : '*',
      d['version'] != null ? d['version'] : 0,
    );
  }
}

export class SubscriptionGroupConfig {
  groupName: string;
  consumeEnable: boolean;
  consumeFromMinEnable: boolean;
  consumeBroadcastEnable: boolean;
  consumeMessageOrderly: boolean;
  retryQueueNums: number;
  retryMaxTimes: number;
  groupRetryPolicy: GroupRetryPolicy;
  brokerId: number;
  whichBrokerWhenConsumeSlowly: number;
  notifyConsumerIdsChangedEnable: boolean;
  groupSysFlag: number;
  consumeTimeoutMinute: number;
  subscriptionDataSet: SimpleSubscriptionData[] | null;
  attributes: Record<string, string>;
  constructor(groupName = '') {
    this.groupName = groupName;
    this.consumeEnable = true;
    this.consumeFromMinEnable = true;
    this.consumeBroadcastEnable = true;
    this.consumeMessageOrderly = false;
    this.retryQueueNums = 1;
    this.retryMaxTimes = 16;
    this.groupRetryPolicy = new GroupRetryPolicy();
    this.brokerId = MASTER_ID;
    this.whichBrokerWhenConsumeSlowly = 1;
    this.notifyConsumerIdsChangedEnable = true;
    this.groupSysFlag = 0;
    this.consumeTimeoutMinute = 15;
    this.subscriptionDataSet = null;
    this.attributes = {};
  }
  toDict(): Record<string, any> {
    const d: Record<string, any> = {
      groupName: this.groupName,
      consumeEnable: this.consumeEnable,
      consumeFromMinEnable: this.consumeFromMinEnable,
      consumeBroadcastEnable: this.consumeBroadcastEnable,
      consumeMessageOrderly: this.consumeMessageOrderly,
      retryQueueNums: this.retryQueueNums,
      retryMaxTimes: this.retryMaxTimes,
      groupRetryPolicy: this.groupRetryPolicy.toDict(),
      brokerId: this.brokerId,
      whichBrokerWhenConsumeSlowly: this.whichBrokerWhenConsumeSlowly,
      notifyConsumerIdsChangedEnable: this.notifyConsumerIdsChangedEnable,
      groupSysFlag: this.groupSysFlag,
      consumeTimeoutMinute: this.consumeTimeoutMinute,
      attributes: this.attributes,
    };
    if (this.subscriptionDataSet != null) {
      d['subscriptionDataSet'] = this.subscriptionDataSet.map((s) => s.toDict());
    }
    return d;
  }
  static fromDict(d: Record<string, any>): SubscriptionGroupConfig {
    const cfg = new SubscriptionGroupConfig(d['groupName'] != null ? d['groupName'] : '');
    cfg.consumeEnable = d['consumeEnable'] !== false;
    cfg.consumeFromMinEnable = d['consumeFromMinEnable'] !== false;
    cfg.consumeBroadcastEnable = d['consumeBroadcastEnable'] !== false;
    cfg.consumeMessageOrderly = d['consumeMessageOrderly'] === true;
    cfg.retryQueueNums = d['retryQueueNums'] != null ? d['retryQueueNums'] : 1;
    cfg.retryMaxTimes = d['retryMaxTimes'] != null ? d['retryMaxTimes'] : 16;
    cfg.groupRetryPolicy = GroupRetryPolicy.fromDict(d['groupRetryPolicy']);
    cfg.brokerId = d['brokerId'] != null ? d['brokerId'] : MASTER_ID;
    cfg.whichBrokerWhenConsumeSlowly = d['whichBrokerWhenConsumeSlowly'] != null ? d['whichBrokerWhenConsumeSlowly'] : 1;
    cfg.notifyConsumerIdsChangedEnable = d['notifyConsumerIdsChangedEnable'] !== false;
    cfg.groupSysFlag = d['groupSysFlag'] != null ? d['groupSysFlag'] : 0;
    cfg.consumeTimeoutMinute = d['consumeTimeoutMinute'] != null ? d['consumeTimeoutMinute'] : 15;
    const rawSub = d['subscriptionDataSet'];
    cfg.subscriptionDataSet = rawSub ? rawSub.map((x: any) => SimpleSubscriptionData.fromDict(x)) : null;
    cfg.attributes = Object.assign({}, d['attributes'] || {});
    return cfg;
  }
  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }
  static decode(data: Buffer): SubscriptionGroupConfig {
    return SubscriptionGroupConfig.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export class SubscriptionGroupWrapper {
  subscriptionGroupTable: Record<string, SubscriptionGroupConfig>;
  forbiddenTable: Record<string, any>;
  dataVersion: Record<string, any>;
  constructor() {
    this.subscriptionGroupTable = {};
    this.forbiddenTable = {};
    this.dataVersion = {};
  }
  toDict(): Record<string, any> {
    const table: Record<string, any> = {};
    for (const [k, v] of Object.entries(this.subscriptionGroupTable)) table[k] = v.toDict();
    return {
      dataVersion: this.dataVersion,
      forbiddenTable: this.forbiddenTable,
      subscriptionGroupTable: table,
    };
  }
  static fromDict(d: Record<string, any>): SubscriptionGroupWrapper {
    const w = new SubscriptionGroupWrapper();
    const table = d['subscriptionGroupTable'] || {};
    for (const [k, v] of Object.entries(table)) {
      w.subscriptionGroupTable[k] = SubscriptionGroupConfig.fromDict(v as Record<string, any>);
    }
    w.forbiddenTable = Object.assign({}, d['forbiddenTable'] || {});
    w.dataVersion = Object.assign({}, d['dataVersion'] || {});
    return w;
  }
  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }
  static decode(data: Buffer): SubscriptionGroupWrapper {
    return SubscriptionGroupWrapper.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

export default { SubscriptionGroupConfig, SubscriptionGroupWrapper, GroupRetryPolicy, GroupRetryPolicyType, SimpleSubscriptionData, MASTER_ID };
