// -*- coding: utf-8 -*-
// Topic route data (org.apache.rocketmq.remoting.protocol.route.*).
// Mirrors python/remoting/protocol/route.py.
import { MessageQueue } from '../common/message.ts';
import { MixAll } from '../common/mixAll.ts';
import { PermName } from '../common/sysflag.ts';
import { RemotingSerializable } from './serialize.ts';

export class QueueData {
  brokerName: string;
  readQueueNums: number;
  writeQueueNums: number;
  perm: number;
  topicSysFlag: number;
  constructor(brokerName = '', readQueueNums = 0, writeQueueNums = 0, perm = 0, topicSysFlag = 0) {
    this.brokerName = brokerName;
    this.readQueueNums = readQueueNums;
    this.writeQueueNums = writeQueueNums;
    this.perm = perm;
    this.topicSysFlag = topicSysFlag;
  }
  clone(): QueueData {
    return new QueueData(this.brokerName, this.readQueueNums, this.writeQueueNums, this.perm, this.topicSysFlag);
  }
  toDict(): Record<string, any> {
    return {
      brokerName: this.brokerName,
      readQueueNums: this.readQueueNums,
      writeQueueNums: this.writeQueueNums,
      perm: this.perm,
      topicSysFlag: this.topicSysFlag,
    };
  }
  static fromDict(d: Record<string, any>): QueueData {
    return new QueueData(
      d['brokerName'] != null ? d['brokerName'] : '',
      parseInt(d['readQueueNums'] != null ? d['readQueueNums'] : 0, 10),
      parseInt(d['writeQueueNums'] != null ? d['writeQueueNums'] : 0, 10),
      parseInt(d['perm'] != null ? d['perm'] : 0, 10),
      parseInt(d['topicSysFlag'] != null ? d['topicSysFlag'] : 0, 10),
    );
  }
}

export class BrokerData {
  cluster: string;
  brokerName: string;
  brokerAddrs: Record<number, string>;
  zoneName: string;
  enableActingMaster: boolean;
  constructor(cluster = '', brokerName = '', brokerAddrs: Record<number, string> | null = null, zoneName = '') {
    this.cluster = cluster;
    this.brokerName = brokerName;
    this.brokerAddrs = brokerAddrs != null ? brokerAddrs : {};
    this.zoneName = zoneName;
    this.enableActingMaster = false;
  }
  selectBrokerAddr(): string | null {
    if (!this.brokerAddrs || Object.keys(this.brokerAddrs).length === 0) return null;
    const master = this.brokerAddrs[0];
    if (master != null) return master;
    const vals = Object.values(this.brokerAddrs);
    return vals.length ? vals[Math.floor(Math.random() * vals.length)] : null;
  }
  clone(): BrokerData {
    return new BrokerData(this.cluster, this.brokerName, Object.assign({}, this.brokerAddrs), this.zoneName);
  }
  toDict(): Record<string, any> {
    const addrs: Record<string, string> = {};
    for (const [k, v] of Object.entries(this.brokerAddrs)) addrs[String(k)] = v;
    return {
      cluster: this.cluster,
      brokerName: this.brokerName,
      brokerAddrs: addrs,
      zoneName: this.zoneName,
      enableActingMaster: this.enableActingMaster,
    };
  }
  static fromDict(d: Record<string, any>): BrokerData {
    const raw = d['brokerAddrs'] || {};
    const addrs: Record<number, string> = {};
    for (const [k, v] of Object.entries(raw)) addrs[parseInt(k, 10)] = v as string;
    const bd = new BrokerData(
      d['cluster'] != null ? d['cluster'] : '',
      d['brokerName'] != null ? d['brokerName'] : '',
      addrs,
      d['zoneName'] != null ? d['zoneName'] : '',
    );
    bd.enableActingMaster = d['enableActingMaster'] === true;
    return bd;
  }
}

export class TopicRouteData {
  orderTopicConf: string | null;
  queueDatas: QueueData[];
  brokerDatas: BrokerData[];
  filterServerTable: Record<string, string[]>;
  topicQueueMappingByBroker: Record<string, any> | null;
  constructor() {
    this.orderTopicConf = null;
    this.queueDatas = [];
    this.brokerDatas = [];
    this.filterServerTable = {};
    this.topicQueueMappingByBroker = null;
  }
  // Assemble all writable MessageQueues (publish side). Mirrors Java
  // MQClientInstance.topicRouteData2TopicPublishInfo.
  getAllMessageQueue(topic = ''): MessageQueue[] {
    const mqs: MessageQueue[] = [];
    for (const qd of this.queueDatas) {
      if (!PermName.checkPerm(qd.perm, PermName.PERM_WRITE)) continue;
      let brokerData: BrokerData | null = null;
      for (const bd of this.brokerDatas) {
        if (bd.brokerName === qd.brokerName) { brokerData = bd; break; }
      }
      if (brokerData == null) continue;
      if (!(MixAll.MASTER_ID in brokerData.brokerAddrs)) continue;
      for (let i = 0; i < qd.writeQueueNums; i++) mqs.push(new MessageQueue(topic, qd.brokerName, i));
    }
    return mqs;
  }
  // Assemble all readable MessageQueues (subscribe side). Mirrors
  // MQClientInstance.topicRouteData2TopicSubscribeInfo (read perm; no master required).
  getAllSubscribeMessageQueue(topic = ''): MessageQueue[] {
    const mqs: MessageQueue[] = [];
    for (const qd of this.queueDatas) {
      if (!PermName.checkPerm(qd.perm, PermName.PERM_READ)) continue;
      for (let i = 0; i < qd.readQueueNums; i++) mqs.push(new MessageQueue(topic, qd.brokerName, i));
    }
    return mqs;
  }
  cloneTopicRouteData(): TopicRouteData {
    const trd = new TopicRouteData();
    trd.orderTopicConf = this.orderTopicConf;
    trd.queueDatas = this.queueDatas.slice();
    trd.brokerDatas = this.brokerDatas.slice();
    const fst: Record<string, string[]> = {};
    for (const [k, v] of Object.entries(this.filterServerTable)) fst[k] = v.slice();
    trd.filterServerTable = fst;
    if (this.topicQueueMappingByBroker != null) trd.topicQueueMappingByBroker = Object.assign({}, this.topicQueueMappingByBroker);
    return trd;
  }
  topicRouteDataChanged(old: TopicRouteData | null): boolean {
    if (old == null) return true;
    const sortQ = (a: QueueData, b: QueueData) =>
      a.brokerName.localeCompare(b.brokerName) ||
      a.readQueueNums - b.readQueueNums ||
      a.writeQueueNums - b.writeQueueNums ||
      a.perm - b.perm;
    const sortB = (a: BrokerData, b: BrokerData) => a.brokerName.localeCompare(b.brokerName);
    const oldQ = this.queueDatas.slice().sort(sortQ);
    const newQ = old.queueDatas.slice().sort(sortQ);
    const oldB = this.brokerDatas.slice().sort(sortB);
    const newB = old.brokerDatas.slice().sort(sortB);
    return !(JSON.stringify(oldQ) === JSON.stringify(newQ) && JSON.stringify(oldB) === JSON.stringify(newB));
  }
  toDict(): Record<string, any> {
    const d: Record<string, any> = {
      orderTopicConf: this.orderTopicConf,
      queueDatas: this.queueDatas.map((q) => q.toDict()),
      brokerDatas: this.brokerDatas.map((b) => b.toDict()),
      filterServerTable: this.filterServerTable,
    };
    if (this.topicQueueMappingByBroker != null) d['topicQueueMappingByBroker'] = this.topicQueueMappingByBroker;
    return d;
  }
  static fromDict(d: Record<string, any>): TopicRouteData {
    const trd = new TopicRouteData();
    trd.orderTopicConf = d['orderTopicConf'] != null ? d['orderTopicConf'] : null;
    trd.queueDatas = (d['queueDatas'] || []).map((q: any) => QueueData.fromDict(q));
    trd.brokerDatas = (d['brokerDatas'] || []).map((b: any) => BrokerData.fromDict(b));
    const fst: Record<string, string[]> = {};
    for (const [k, v] of Object.entries(d['filterServerTable'] || {})) fst[k] = (v as any[]).slice();
    trd.filterServerTable = fst;
    trd.topicQueueMappingByBroker = d['topicQueueMappingByBroker'] != null ? d['topicQueueMappingByBroker'] : null;
    return trd;
  }
  encode(): Buffer {
    const data = RemotingSerializable.encode(this.toDict());
    return data != null ? data : Buffer.alloc(0);
  }
  static decode(data: Buffer): TopicRouteData {
    return TopicRouteData.fromDict(RemotingSerializable.decode(data) as Record<string, any>);
  }
}

// ---------------------------------------------------------------------------
// TopicRouteData helper functions (Java TopicRouteData / MQHelper).
// ---------------------------------------------------------------------------

// Map<brokerName, Map<brokerId, addr>>.
export function brokerDataList2Map(bdList: BrokerData[]): Record<string, Record<number, string>> {
  const map: Record<string, Record<number, string>> = {};
  if (bdList != null) {
    for (const bd of bdList) {
      map[bd.brokerName] = Object.assign({}, bd.brokerAddrs);
    }
  }
  return map;
}

export function brokerData2Json(bd: BrokerData): string {
  return RemotingSerializable.toJson(bd.toDict());
}

export function topicRouteData2TopicRouteDataJson(trd: TopicRouteData): string {
  return RemotingSerializable.toJson(trd.toDict());
}

// Find the master (brokerId 0) address for a broker within the route.
export function findBrokerAddressInPublish(brokerName: string, trd: TopicRouteData): string | null {
  if (!brokerName) return null;
  const map = brokerDataList2Map(trd.brokerDatas)[brokerName];
  if (map) return map[MixAll.MASTER_ID] != null ? map[MixAll.MASTER_ID] : null;
  return null;
}

export function findBrokerAddressInSubscribe(
  brokerName: string, brokerId: number, onlyThisBroker: boolean, trd: TopicRouteData,
): string | null {
  if (!brokerName || brokerName.length < 1) return null;
  const map = brokerDataList2Map(trd.brokerDatas)[brokerName];
  if (map && Object.keys(map).length) {
    const addr = map[brokerId];
    if (addr != null) return addr;
    if (!onlyThisBroker) {
      for (const [k, v] of Object.entries(map)) {
        const kid = parseInt(k, 10);
        if (brokerId !== MixAll.MASTER_ID) {
          if (kid !== MixAll.MASTER_ID) return v;
        } else {
          if (kid === MixAll.MASTER_ID) return v;
        }
      }
      const firstKey = Object.keys(map)[0];
      return firstKey != null ? map[parseInt(firstKey, 10)] : null;
    }
  }
  return null;
}

export function findBrokerAddressInNotRepeat(brokerName: string, trd: TopicRouteData): string | null {
  if (!brokerName) return null;
  const map = brokerDataList2Map(trd.brokerDatas)[brokerName];
  if (map && Object.keys(map).length) {
    const master = map[MixAll.MASTER_ID];
    if (master != null) return master;
    const firstKey = Object.keys(map)[0];
    return firstKey != null ? map[parseInt(firstKey, 10)] : null;
  }
  return null;
}

// A broker is "deployed in container" when its master address has no host:port
// (it is referenced by a service/DNS name rather than an IP).
export function brokerDeployInContainer(brokerName: string, trd: TopicRouteData): boolean {
  if (brokerName != null && trd != null) {
    for (const bd of trd.brokerDatas) {
      if (brokerName === bd.brokerName) {
        const addr = bd.brokerAddrs[MixAll.MASTER_ID];
        return addr != null && addr.indexOf(':') < 0;
      }
    }
  }
  return false;
}

export default { QueueData, BrokerData, TopicRouteData, brokerDataList2Map, brokerData2Json, topicRouteData2TopicRouteDataJson, findBrokerAddressInPublish, findBrokerAddressInSubscribe, findBrokerAddressInNotRepeat, brokerDeployInContainer };
