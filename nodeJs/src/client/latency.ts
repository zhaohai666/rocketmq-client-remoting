// -*- coding: utf-8 -*-
// Latency fault tolerance (org.apache.rocketmq.client.latency.*).
// Faithful port of python/rocketmq/client/latency.py.
//
// MQFaultStrategy decides which broker queue to use next, avoiding brokers that have recently
// shown high latency / were isolated after a failed send (when sendLatencyFaultEnable is on).
import { MessageQueue } from '../common/message.ts';

export class FaultItem {
  name: string;
  currentLatency: number;
  startTimestamp: number;
  reachableFlag: boolean;

  constructor(name: string) {
    this.name = name;
    this.currentLatency = 0;
    this.startTimestamp = 0;
    this.reachableFlag = true;
  }

  // Mark the broker unavailable until (now + duration).
  updateNotAvailableDuration(duration: number): void {
    this.startTimestamp = Date.now() + duration;
  }

  isAvailable(): boolean {
    return Date.now() >= this.startTimestamp;
  }

  isReachable(): boolean {
    return this.reachableFlag;
  }
}

export class LatencyFaultToleranceImpl {
  faultItemTable: Record<string, FaultItem> = {};

  updateFaultItem(name: string, currentLatency: number, startTimestamp: number, reachable: boolean): void {
    let item = this.faultItemTable[name];
    if (item == null) {
      item = new FaultItem(name);
      this.faultItemTable[name] = item;
    }
    item.currentLatency = currentLatency;
    item.startTimestamp = startTimestamp;
    item.reachableFlag = reachable;
  }

  isAvailable(name: string): boolean {
    const item = this.faultItemTable[name];
    if (item != null) return item.isAvailable();
    return true;
  }

  isReachable(name: string | null): boolean {
    if (name == null) return false;
    const item = this.faultItemTable[name];
    if (item != null) return item.isReachable();
    return true;
  }

  remove(name: string): void {
    delete this.faultItemTable[name];
  }

  getFaultItem(name: string): FaultItem | null {
    return this.faultItemTable[name] != null ? this.faultItemTable[name] : null;
  }
}

export class MQFaultStrategy {
  static LATENCY_MAX = [50, 100, 550, 1800, 3000, 5000, 15000];
  static NOT_AVAILABLE_DURATION = [0, 0, 2000, 5000, 6000, 10000, 30000];

  latencyMax: number[];
  notAvailableDuration: number[];
  sendLatencyFaultEnable: boolean;
  latencyFaultTolerance: LatencyFaultToleranceImpl;

  constructor(sendLatencyFaultEnable: boolean = false) {
    this.latencyMax = MQFaultStrategy.LATENCY_MAX;
    this.notAvailableDuration = MQFaultStrategy.NOT_AVAILABLE_DURATION;
    this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    this.latencyFaultTolerance = new LatencyFaultToleranceImpl();
  }

  selectOneMessageQueue(tpInfo: any, lastBrokerName: string | null = null, resetIndex: boolean = false): MessageQueue | null {
    if (!this.sendLatencyFaultEnable) {
      return tpInfo.selectOneMessageQueue(lastBrokerName, resetIndex);
    }
    const mqs: MessageQueue[] = tpInfo.msgQueueList || [];
    for (let i = 0; i < mqs.length; i++) {
      const mq = tpInfo.selectOneMessageQueue(lastBrokerName, resetIndex);
      if (this.latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
        return mq;
      }
    }
    if (this.latencyFaultTolerance.isReachable(lastBrokerName)) {
      return tpInfo.selectOneMessageQueue(lastBrokerName, resetIndex);
    }
    return tpInfo.selectOneMessageQueue(null, resetIndex);
  }

  updateFaultItem(brokerName: string, currentLatency: number, isolation: boolean, reachable: boolean = true): void {
    if (isolation) {
      const duration = this._computeNotAvailableDuration(Math.max(currentLatency, 30000));
      this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, Date.now() + duration, false);
    } else {
      this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, Date.now(), reachable);
    }
  }

  _computeNotAvailableDuration(currentLatency: number): number {
    for (let i = this.latencyMax.length - 1; i >= 0; i--) {
      if (currentLatency >= this.latencyMax[i]) {
        return this.notAvailableDuration[i];
      }
    }
    return 0;
  }
}

export default { FaultItem, LatencyFaultToleranceImpl, MQFaultStrategy };
