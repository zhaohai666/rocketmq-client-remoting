// -*- coding: utf-8 -*-
// SubscriptionData + FilterAPI (org.apache.rocketmq.common.filter.*).
import { javaStringHash, currentTimeMillis } from './utilAll.ts';

export const ExpressionType = {
  TAG: 'TAG',
  SQL92: 'SQL92',
  CLASS_FILTER: 'CLASS_FILTER',
};

export class SubscriptionData {
  constructor(topic = null, subString = null) {
    this.classFilterMode = false;
    this.topic = topic;
    this.subString = subString;
    this.tagsSet = new Set();
    this.codeSet = new Set();
    this.subVersion = currentTimeMillis();
    this.expressionType = ExpressionType.TAG;
    this.filterClassSource = null;
  }

  toDict() {
    return {
      classFilterMode: this.classFilterMode,
      topic: this.topic,
      subString: this.subString,
      tagsSet: Array.from(this.tagsSet).sort(),
      codeSet: Array.from(this.codeSet).sort((a, b) => a - b),
      subVersion: this.subVersion,
      expressionType: this.expressionType,
    };
  }
}

export const FilterAPI = {
  SUB_ALL: '*',

  // Mirror Java FilterAPI.buildSubscriptionData (verified probe vectors in memory).
  buildSubscriptionData(topic, subString) {
    const sub = new SubscriptionData(topic, subString);
    if (subString == null || subString === '' || subString === FilterAPI.SUB_ALL) {
      sub.subString = FilterAPI.SUB_ALL;
      return sub;
    }
    let parts = subString.split('||');
    while (parts.length && parts[parts.length - 1] === '') parts.pop();
    if (parts.length === 0) throw new Error('subString split error');
    for (const part of parts) {
      const tag = part.trim();
      if (tag !== '') {
        sub.tagsSet.add(tag);
        sub.codeSet.add(javaStringHash(tag));
      }
    }
    return sub;
  },

  // Java FilterAPI.build(topic, subString, type). The distinction from
  // buildSubscriptionData matters on the wire: TAG goes through the
  // tag/codeSet hash expansion above, while SQL92 / CLASS_FILTER take a
  // SEPARATE branch that leaves tagsSet and codeSet EMPTY. Populating them for
  // a SQL92 expression makes the broker hash "a = 'ok'" as if it were a tag
  // and intersect the pull with that fake code, so the consumer receives
  // nothing at all — the subscription looks registered and silently starves.
  build(topic, subString, type) {
    if (type == null || type === ExpressionType.TAG) {
      return this.buildSubscriptionData(topic, subString);
    }
    if (subString == null || subString === '') {
      throw new Error(`Expression can't be null! ${type}`);
    }
    const sub = new SubscriptionData(topic, subString);
    sub.expressionType = type;
    return sub;
  },
};

export default { ExpressionType, SubscriptionData, FilterAPI };
