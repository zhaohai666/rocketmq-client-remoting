// -*- coding: utf-8 -*-
// Latency fault tolerance.
//
// MQFaultStrategy decides which broker queue to pick next, avoiding brokers
// that were isolated after recent high latency or send failures (provided
// sendLatencyFaultEnable is on).
import { connect as netConnect } from 'node:net';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.latency');

// Pluggable reachability probe. May return a boolean directly or a Promise<boolean> (node's default detector is the promise version).
export type ServiceDetector = (addr: string, timeoutMillis: number) => boolean | Promise<boolean>;
// Resolves a broker name to a probeable address.
export type BrokerResolver = (brokerName: string) => string | null;

// node's default detector: a single pure TCP connect probe. It runs only on the detector timer and never on the data path.
export function tcpDetect(addr: string, timeoutMillis: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const sep = addr.lastIndexOf(':');
    if (sep < 0) { resolve(false); return; }
    const host = addr.substring(0, sep);
    const port = parseInt(addr.substring(sep + 1), 10);
    if (!Number.isFinite(port) || port <= 0) { resolve(false); return; }
    let settled = false;
    const finish = (ok: boolean) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch (e) { /* ignore */ }
      resolve(ok);
    };
    const sock = netConnect(port, host);
    sock.setTimeout(timeoutMillis);
    sock.once('connect', () => finish(true));
    sock.once('timeout', () => finish(false));
    sock.once('error', () => finish(false));
  });
}

export class FaultItem {
  name: string;
  currentLatency: number;
  startTimestamp: number;
  reachableFlag: boolean;
  // The next moment the detector may re-probe this item.
  checkStamp: number;

  constructor(name: string) {
    this.name = name;
    this.currentLatency = 0;
    this.startTimestamp = 0;
    this.reachableFlag = true;
    this.checkStamp = 0;
  }

  // Mark the broker unavailable until (now + duration). Only EXTENDS an
  // existing isolation window, never shortens it (updates only when
  // `duration > 0 && now + duration > start`).
  updateNotAvailableDuration(duration: number): void {
    if (duration > 0 && Date.now() + duration > this.startTimestamp) {
      this.startTimestamp = Date.now() + duration;
      logger.info('%s will be isolated for %d ms.', this.name, duration);
    }
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
  // Defaults: probe timeout 200ms, per-item probe interval 2s, detector switch off.
  detectTimeout = 200;
  detectInterval = 2000;
  startDetectorEnable = false;
  resolver: BrokerResolver | null = null;
  serviceDetector: ServiceDetector | null = null;
  private _detectorTimer: NodeJS.Timeout | null = null;

  updateFaultItem(name: string, currentLatency: number, notAvailableDuration: number, reachable: boolean): void {
    let item = this.faultItemTable[name];
    if (item == null) {
      item = new FaultItem(name);
      this.faultItemTable[name] = item;
    }
    item.currentLatency = currentLatency;
    item.updateNotAvailableDuration(notAvailableDuration);
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

  setDetectTimeout(timeout: number): void { this.detectTimeout = timeout; }
  setDetectInterval(interval: number): void { this.detectInterval = interval; }
  setStartDetectorEnable(enable: boolean): void { this.startDetectorEnable = enable; }
  setResolver(resolver: BrokerResolver | null): void { this.resolver = resolver; }
  setServiceDetector(detector: ServiceDetector | null): void { this.serviceDetector = detector; }

  // detectByOneRound: walk every item whose checkStamp is due, pushing the
  // stamp forward FIRST before probing, so one slow probe cannot re-trigger on
  // the same item. node's default detector is promise-based, so reachability
  // flips land in the promise callback; a synchronous detector flips
  // immediately.
  detectByOneRound(): void {
    for (const key of Object.keys(this.faultItemTable)) {
      const brokerItem = this.faultItemTable[key];
      if (brokerItem == null) continue;
      if (Date.now() - brokerItem.checkStamp >= 0) {
        brokerItem.checkStamp = Date.now() + this.detectInterval;
        const brokerAddr = this.resolver != null ? this.resolver(brokerItem.name) : null;
        if (brokerAddr == null) {
          delete this.faultItemTable[key];
          continue;
        }
        if (this.serviceDetector == null) continue;
        const serviceOK = this.serviceDetector(brokerAddr, this.detectTimeout);
        Promise.resolve(serviceOK).then((ok) => {
          if (ok && !brokerItem.reachableFlag) {
            logger.info('%s is reachable now, then it can be used.', brokerItem.name);
            brokerItem.reachableFlag = true;
          }
        }).catch(() => { /* probe failure is not the client's failure */ });
      }
    }
  }

  // Start the detector: a fixed 3s first round and 3s period; each tick does work only when startDetectorEnable is true.
  startDetector(): void {
    if (this._detectorTimer != null) return;
    const tick = () => {
      try {
        if (this.startDetectorEnable) this.detectByOneRound();
      } catch (e) {
        logger.warning('unexpected exception raised while detecting service reachability: %s', (e as Error).message);
      }
      this._detectorTimer = setTimeout(tick, 3000);
      if (typeof this._detectorTimer.unref === 'function') this._detectorTimer.unref();
    };
    this._detectorTimer = setTimeout(tick, 3000);
    if (typeof this._detectorTimer.unref === 'function') this._detectorTimer.unref();
  }

  shutdown(): void {
    if (this._detectorTimer != null) {
      clearTimeout(this._detectorTimer);
      this._detectorTimer = null;
    }
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

  // updateFaultItem: takes effect only when sendLatencyFaultEnable is on; the
  // isolation duration comes from the latency table, and in the isolation
  // state currentLatency is treated as a FIXED 10000ms (not the 30s lower
  // bound): `computeNotAvailableDuration(isolation ? 10000 : currentLatency)`.
  updateFaultItem(brokerName: string, currentLatency: number, isolation: boolean, reachable: boolean = true): void {
    if (!this.sendLatencyFaultEnable) return;
    const duration = this._computeNotAvailableDuration(isolation ? 10000 : currentLatency);
    this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
  }

  _computeNotAvailableDuration(currentLatency: number): number {
    for (let i = this.latencyMax.length - 1; i >= 0; i--) {
      if (currentLatency >= this.latencyMax[i]) {
        return this.notAvailableDuration[i];
      }
    }
    return 0;
  }

  // ---- startDetector-related switches and injection ----
  isStartDetectorEnable(): boolean {
    return this.latencyFaultTolerance.startDetectorEnable;
  }

  setStartDetectorEnable(enable: boolean): void {
    this.latencyFaultTolerance.setStartDetectorEnable(enable);
  }

  setResolver(resolver: BrokerResolver | null): void {
    this.latencyFaultTolerance.setResolver(resolver);
  }

  setServiceDetector(detector: ServiceDetector | null): void {
    this.latencyFaultTolerance.setServiceDetector(detector);
  }

  startDetector(): void {
    this.latencyFaultTolerance.startDetector();
  }

  shutdown(): void {
    this.latencyFaultTolerance.shutdown();
  }
}

export default { FaultItem, LatencyFaultToleranceImpl, MQFaultStrategy };
