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
  // Heartbeat V2 pair: `heartbeatFingerprint` identifies this client's
  // subscription snapshot and `withoutSub` asks the broker to skip the
  // (unchanged) subscription registration. Both stay at their zero value on a
  // V1 beat, which is what makes a V1 body indistinguishable from before.
  heartbeatFingerprint: number;
  constructor(clientID = '') {
    this.clientID = clientID;
    this.producerDataSet = [];
    this.consumerDataSet = [];
    this.withoutSub = false;
    this.heartbeatFingerprint = 0;
  }
  toDict(): Record<string, any> {
    return {
      clientID: this.clientID,
      producerDataSet: this.producerDataSet.map((p) => p.toDict()),
      consumerDataSet: this.consumerDataSet.map((c) => c.toDict()),
      heartbeatFingerprint: this.heartbeatFingerprint,
      withoutSub: this.withoutSub,
    };
  }
  static fromDict(d: Record<string, any>): HeartbeatData {
    const hb = new HeartbeatData(d['clientID'] != null ? d['clientID'] : '');
    for (const p of d['producerDataSet'] || []) hb.producerDataSet.push(ProducerData.fromDict(p));
    for (const c of d['consumerDataSet'] || []) hb.consumerDataSet.push(ConsumerData.fromDict(c));
    hb.withoutSub = d['withoutSub'] === true;
    hb.heartbeatFingerprint = parseInt(d['heartbeatFingerprint'] != null ? d['heartbeatFingerprint'] : 0, 10) || 0;
    return hb;
  }
  encode(): Buffer {
    return RemotingSerializable.encode(this.toDict());
  }
  static decode(data: Buffer): HeartbeatData {
    return HeartbeatData.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }

  // The same body with every subscription set emptied and withoutSub flagged, so a
  // V2 broker can skip subscription registration it already has. Nothing shared
  // with the receiver is mutated: the ConsumerData entries are fresh values.
  cloneWithoutSubscriptions(): HeartbeatData {
    const out = new HeartbeatData(this.clientID);
    out.heartbeatFingerprint = this.heartbeatFingerprint;
    out.withoutSub = true;
    for (const c of this.consumerDataSet) {
      const cd = new ConsumerData(c.groupName, c.consumeType, c.messageModel, c.consumeFromWhere);
      cd.unitMode = c.unitMode;
      cd.subscriptionDataSet = [];
      out.consumerDataSet.push(cd);
    }
    for (const p of this.producerDataSet) {
      const pd = new ProducerData(p.groupName);
      pd.enable = p.enable;
      out.producerDataSet.push(pd);
    }
    return out;
  }

  // Fingerprint of this body's subscription snapshot: a hash of a NORMALISED
  // rendering, with the fields that change per beat or per client stripped out
  // (every subscription's subVersion, clientID, withoutSub, heartbeatFingerprint)
  // and every set put in a defined order.
  //
  // Ordering matters more here than it does in a single-threaded reference
  // implementation: registration order is not stable across beats in this
  // runtime, so a rendering that kept it would report a subscription change on
  // every heartbeat and the broker-side skip would never trigger.
  //
  // The number only has to be stable for THIS client while its subscriptions do
  // not change and different as soon as they do — a broker compares one client's
  // beats to each other, never across languages.
  computeHeartbeatFingerprint(): number {
    // Code-unit comparison, not localeCompare: a locale-sensitive collation would
    // make the same subscriptions hash differently on two machines.
    const byCodeUnit = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);
    const consumers = this.consumerDataSet
      .map((c) => {
        const d = c.toDict();
        // SubscriptionData.toDict already emits tagsSet/codeSet in a defined
        // order; only subVersion has to be normalised out — it is stamped with
        // the registration time, so it changes on every beat.
        d['subscriptionDataSet'] = (d['subscriptionDataSet'] || [])
          .map((s: Record<string, any>) => { s['subVersion'] = 0; return s; })
          .sort((a: Record<string, any>, b: Record<string, any>) =>
            byCodeUnit(String(a['topic']), String(b['topic'])));
        return d;
      })
      .sort((a: Record<string, any>, b: Record<string, any>) =>
        byCodeUnit(String(a['groupName']), String(b['groupName'])));
    const producers = this.producerDataSet
      .map((p) => p.toDict())
      .sort((a: Record<string, any>, b: Record<string, any>) =>
        byCodeUnit(String(a['groupName']), String(b['groupName'])));
    return javaStringHash(JSON.stringify({
      clientID: '',
      producerDataSet: producers,
      consumerDataSet: consumers,
      heartbeatFingerprint: 0,
      withoutSub: false,
    }));
  }
}

export function heartbeatDataEncode(hd: HeartbeatData): Buffer {
  return RemotingSerializable.encode(hd.toDict());
}

export function heartbeatDataDecode(bs: Buffer): HeartbeatData {
  return HeartbeatData.fromDict(RemotingSerializable.decode(bs) as Record<string, any>);
}

// ---------------- heartbeat V2 ----------------

// Ext-field names a V2-capable broker sets on the HEART_BEAT(34) response. Both
// travel as `true` / `false` TEXT, so a reader that defaults to false is the
// correct answer for a broker that never heard of V2.
export const heartbeatExtIsSupportV2 = 'IS_SUPPORT_HEART_BEAT_V2';
export const heartbeatExtIsSubChange = 'IS_SUB_CHANGE';

export interface HeartbeatV2Flags {
  version: number;
  supportV2: boolean;
  subChange: boolean;
}

// Read the two V2 flags + the broker version off a heartbeat response. Missing
// ext fields (every pre-V2 broker, every stub) mean "not V2-capable", which keeps
// the client sending subscription-carrying bodies. Typed structurally so this
// module stays free of a remotingCommand import.
export function decodeHeartbeatV2Flags(
  response: { version?: number; extFields?: Record<string, string> | null } | null): HeartbeatV2Flags {
  const out: HeartbeatV2Flags = { version: response != null ? response.version : 0, supportV2: false, subChange: false };
  if (response == null) return out;
  const ext = response.extFields || {};
  const read = (key: string): boolean =>
    String(ext[key] == null ? '' : ext[key]).toLowerCase() === 'true';
  out.supportV2 = read(heartbeatExtIsSupportV2);
  out.subChange = read(heartbeatExtIsSubChange);
  return out;
}

// Java String.hashCode over UTF-16 code units, kept in signed 32-bit range. The
// fingerprint must use THIS hash and not a JS-number hash: charCodeAt yields code
// units, which is exactly what the reference model iterates over.
export function javaStringHash(text: string): number {
  let h = 0;
  for (let i = 0; i < text.length; i++) {
    h = (Math.imul(31, h) + text.charCodeAt(i)) | 0;
  }
  return h;
}

export default {
  HeartbeatData, ConsumerData, ProducerData, heartbeatDataEncode, heartbeatDataDecode,
  ConsumeType, MessageModel, ConsumeFromWhere,
  heartbeatExtIsSupportV2, heartbeatExtIsSubChange, decodeHeartbeatV2Flags, javaStringHash,
};
