import { serverStringHash, currentTimeMillis } from './utilAll.ts';

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
        sub.codeSet.add(serverStringHash(tag));
      }
    }
    return sub;
  },

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
