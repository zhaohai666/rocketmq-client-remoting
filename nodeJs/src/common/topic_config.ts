// -*- coding: utf-8 -*-
// TopicConfig (org.apache.rocketmq.common.TopicConfig).
// Faithful port of python/common/topic_config.py.

import { PermName } from './sysflag.ts';

// org.apache.rocketmq.common.TopicConfig.defaultReadQueueNums / defaultWriteQueueNums
export const DEFAULT_READ_QUEUE_NUMS = 16;
export const DEFAULT_WRITE_QUEUE_NUMS = 16;
// PermName.PERM_READ | PermName.PERM_WRITE
export const DEFAULT_PERM = 6;

export const TopicFilterType = {
  SINGLE_TAG: 'SINGLE_TAG',
  MULTI_TAG: 'MULTI_TAG',
};

export class TopicConfig {
  topic = '';
  readQueueNums = DEFAULT_READ_QUEUE_NUMS;
  writeQueueNums = DEFAULT_WRITE_QUEUE_NUMS;
  perm = DEFAULT_PERM;
  topicFilterType = TopicFilterType.SINGLE_TAG;
  topicSysFlag = 0;
  order = false;
  attributes: Record<string, string> = {};

  constructor(
    topic = '',
    readQueueNums = DEFAULT_READ_QUEUE_NUMS,
    writeQueueNums = DEFAULT_WRITE_QUEUE_NUMS,
    perm = DEFAULT_PERM,
    topicFilterType = TopicFilterType.SINGLE_TAG,
    topicSysFlag = 0,
    order = false,
    attributes: Record<string, string> | null = null,
  ) {
    this.topic = topic;
    this.readQueueNums = readQueueNums;
    this.writeQueueNums = writeQueueNums;
    this.perm = perm;
    this.topicFilterType = topicFilterType;
    this.topicSysFlag = topicSysFlag;
    this.order = order;
    this.attributes = Object.assign({}, attributes || {});
  }

  toDict() {
    return {
      topicName: this.topic,
      readQueueNums: this.readQueueNums,
      writeQueueNums: this.writeQueueNums,
      perm: this.perm,
      topicFilterType: this.topicFilterType,
      topicSysFlag: this.topicSysFlag,
      order: this.order,
      attributes: this.attributes,
    };
  }

  static fromDict(data: any) {
    data = data || {};
    return new TopicConfig(
      data.topicName != null ? data.topicName : '',
      data.readQueueNums != null ? data.readQueueNums : DEFAULT_READ_QUEUE_NUMS,
      data.writeQueueNums != null ? data.writeQueueNums : DEFAULT_WRITE_QUEUE_NUMS,
      data.perm != null ? data.perm : DEFAULT_PERM,
      data.topicFilterType != null ? data.topicFilterType : TopicFilterType.SINGLE_TAG,
      data.topicSysFlag != null ? data.topicSysFlag : 0,
      data.order != null ? data.order : false,
      data.attributes ? Object.assign({}, data.attributes) : null,
    );
  }

  encode(): string {
    return JSON.stringify(this.toDict());
  }

  static decode(jsonStr: string): TopicConfig {
    return TopicConfig.fromDict(JSON.parse(jsonStr));
  }

  equals(other: any): boolean {
    if (this === other) return true;
    if (!(other instanceof TopicConfig)) return false;
    return this.topic === other.topic &&
      this.readQueueNums === other.readQueueNums &&
      this.writeQueueNums === other.writeQueueNums &&
      this.perm === other.perm &&
      this.topicFilterType === other.topicFilterType &&
      this.topicSysFlag === other.topicSysFlag &&
      this.order === other.order &&
      JSON.stringify(this.attributes) === JSON.stringify(other.attributes);
  }

  toString() {
    return `TopicConfig[topicName=${this.topic}, readQueueNums=${this.readQueueNums}, ` +
      `writeQueueNums=${this.writeQueueNums}, perm=${PermName.permToString(this.perm)}]`;
  }
}

export class TopicConfigSerializeWrapper {
  topicConfig: TopicConfig = new TopicConfig();
  attributes: Record<string, string> = {};

  constructor(topicConfig: TopicConfig | null = null, attributes: Record<string, string> | null = null) {
    if (topicConfig) this.topicConfig = topicConfig;
    this.attributes = Object.assign({}, attributes || {});
  }

  toDict() {
    return {
      topicConfig: this.topicConfig.toDict(),
      attributes: this.attributes,
    };
  }

  static fromDict(data: any): TopicConfigSerializeWrapper {
    data = data || {};
    const wrapper = new TopicConfigSerializeWrapper();
    if (data.topicConfig) wrapper.topicConfig = TopicConfig.fromDict(data.topicConfig);
    wrapper.attributes = Object.assign({}, data.attributes || {});
    return wrapper;
  }

  encode(): string {
    return JSON.stringify(this.toDict());
  }

  static decode(jsonStr: string): TopicConfigSerializeWrapper {
    return TopicConfigSerializeWrapper.fromDict(JSON.parse(jsonStr));
  }
}

export default {
  TopicConfig,
  TopicFilterType,
  TopicConfigSerializeWrapper,
  DEFAULT_READ_QUEUE_NUMS,
  DEFAULT_WRITE_QUEUE_NUMS,
  DEFAULT_PERM,
};
