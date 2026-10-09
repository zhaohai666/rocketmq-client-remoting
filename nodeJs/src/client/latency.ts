// -*- coding: utf-8 -*-
// Latency fault tolerance (org.apache.rocketmq.client.latency.*).
// Faithful port of python/client/latency.py, aligned with the Java 5.5.1
// MQFaultStrategy / LatencyFaultToleranceImpl (incl. the startDetector probe).
//
// MQFaultStrategy decides which broker queue to use next, avoiding brokers that have recently
// shown high latency / were isolated after a failed send (when sendLatencyFaultEnable is on).
import { connect as netConnect } from 'node:net';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.latency');

// Java LatencyFaultToleranceImpl.ServiceDetector — pluggable reachability probe.
// May return a boolean directly or a Promise<boolean> (the node default probe).
export type ServiceDetector = (addr: string, timeoutMillis: number) => boolean | Promise<boolean>;
// Java LatencyFaultTolerance.Resolver — broker name -> a probeable address.
export type BrokerResolver = (brokerName: string) => string | null;

// The node default detector: a plain TCP connect probe (Java plugs a
// NettyRemoteServiceDetector doing the same connect check remotely). Runs on
// the detector timer only — never on the data path.
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
  // Java FaultItem.checkStamp: the next time the detector may probe this item.
  checkStamp: number;

  constructor(name: string) {
    this.name = name;
    this.currentLatency = 0;
    this.startTimestamp = 0;
    this.reachableFlag = true;
    this.checkStamp = 0;
  }

  // Mark the broker unavailable until (now + duration). Java only EXTENDS an
  // existing isolation window (LatencyFaultToleranceImpl.FaultItem.
  // updateNotAvailableDuration: `if (duration > 0 && now + duration > start)`).
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
  // Java defaults (LatencyFaultToleranceImpl fields): 200ms probe timeout,
  // 2s per-item probe interval, detector off.
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

  // Java detectByOneRound: probe every item whose checkStamp has come due,
  // re-marking the stamp first so a slow probe cannot loop on the same item.
  // The node default detector is promise-based, so the reachability flip lands
  // in the probe's continuation (Java blocks its scheduled thread instead).
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

  // Java startDetector: scheduleAtFixedRate(3s initial, 3s interval); each run
  // only acts when startDetectorEnable is set.
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

  // Java updateFaultItem: only active when sendLatencyFaultEnable; the isolation
  // window comes from the latency table over a FIXED 10000ms probe latency when
  // isolating (NOT the 30s floor the old code used — Java
  // MQFaultStrategy.updateFaultItem: `computeNotAvailableDuration(isolation ? 10000 : currentLatency)`).
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

  // ---- startDetector plumbing (Java MQFaultStrategy implements StartAndShutdown) ----
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
