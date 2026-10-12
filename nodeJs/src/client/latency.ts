// -*- coding: utf-8 -*-
// 延迟故障容忍（latency fault tolerance）。
//
// MQFaultStrategy 决定下一次挑选哪个 broker 队列，避开最近高延迟或发送失败后被
// 隔离的 broker（前提是 sendLatencyFaultEnable 打开）。
import { connect as netConnect } from 'node:net';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.latency');

// 可插拔的可达性探测。可以直接返回布尔值，也可以返回 Promise<boolean>（node 默认探测器就是 promise 版本）。
export type ServiceDetector = (addr: string, timeoutMillis: number) => boolean | Promise<boolean>;
// broker 名 -> 一个可探测地址的解析器。
export type BrokerResolver = (brokerName: string) => string | null;

// node 默认探测器：一次纯 TCP 连接探测。只在 detector 定时器上跑，绝对不进数据面。
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
  // 探测器下一次允许重新探测该项的时刻。
  checkStamp: number;

  constructor(name: string) {
    this.name = name;
    this.currentLatency = 0;
    this.startTimestamp = 0;
    this.reachableFlag = true;
    this.checkStamp = 0;
  }

  // 把 broker 标为不可用直到 (now + duration)。**只会延长**已有的隔离窗口，
  // 不会缩短（`duration > 0 && now + duration > start` 才更新）。
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
  // 默认值：探测超时 200ms，单项探测间隔 2s，探测开关关。
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

  // detectByOneRound：遍历每个 checkStamp 到点的项，**先把 stamp 往后推**再发起探测，
  // 这样一次慢探测不会在同一项上反复触发。node 默认探测器基于 promise，可达性翻转
  // 落在 promise 回调里；同步 detector 会当场翻转。
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

  // 启动 detector：固定 3s 首轮、3s 周期；每次 tick 只在 startDetectorEnable 为 true 时干活。
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

  // updateFaultItem：仅在 sendLatencyFaultEnable 打开时生效；隔离时长取自延迟表，
  // 隔离态下把 currentLatency 视为**固定的 10000ms**（并非 30s 的下限）：
  // `computeNotAvailableDuration(isolation ? 10000 : currentLatency)`。
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

  // ---- startDetector 相关的开关与注入 ----
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
