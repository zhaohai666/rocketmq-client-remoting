// -*- coding: utf-8 -*-
// 消费端统计（ConsumerStatsManager + StatsItem/StatsItemSet/StatsSnapshot）。
//
// 真实的模型（**不是**"每分钟一个桶"这个名字暗示的样子）：
//
// * 一个 StatsItem 里维护**累计**的 value/times（只增不减），并挂两条采样快照链：
//   `csListMinute`（每 10s 落一个累计点）和 `csListHour`（每 10 分钟落一个累计点）；
// * 快照计算走 computeStatsData：
//       sum       = last.value - first.value          (窗口差)
//       tps       = sum * 1000.0 / (last.ts - first.ts)  (每秒)
//       timesDiff = last.times - first.times
//       avgpt     = timesDiff > 0 ? sum / timesDiff : 0  ("平均每次") —— RT 类
//                   指标上就是平均耗时（ms）
// * TPS 类计数器走 addValue(key, msgs, 1)：value 累计消息数、times 累计调用次数 →
//   tps = 每秒的调用（或消息）数；
// * RT 类计数器走 addRTValue(key, rt, 1)：value 累计耗时 ms、times 累计调用次数
//   → avgpt = 平均耗时；
// * consumeStatus(group, topic) 除了 consumeFailedMsgs 之外全部读 MINUTE 快照；
//   consumeFailedMsgs 取的是失败集合的 HOUR 窗口 sum ——**故意跨窗口**，是刻意的
//   设计，别"顺手改正"。
//
// 实现细节的差异（语义一致）：每个 StatsItem 原本各自挂一个 10s/10min 采样器；这里
// 用**管理器级别的一把**采样定时器统一遍历所有集合——采样精度不变（10s），少掉几十
// 个定时器。
import { ConsumeStatus } from '../remoting/bodies.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('consumer-stats');

// 采样间隔：minute 每 10s 一点，hour 每 10min 一点。
export const SAMPLING_INTERVAL_SECONDS = 10.0;
export const HOUR_SAMPLING_INTERVAL_SECONDS = 600.0;
// 快照链长度上限：约 60 个点 ≈ 10 分钟窗口（分钟链）。
const MINUTE_LIST_MAX = 60;
const HOUR_LIST_MAX = 60;

export class StatsSnapshot {
  sum = 0;
  tps = 0.0;
  avgpt = 0.0;
  times = 0;
}

interface SamplePoint {
  ts: number;      // timestamp ms
  value: number;   // cumulative value
  times: number;   // cumulative times
}

// 快照计算：窗口首尾差值 -> sum / tps / timesDiff / avgpt。
export function computeStatsData(csList: SamplePoint[]): StatsSnapshot {
  const ss = new StatsSnapshot();
  if (csList.length === 0) return ss;
  const first = csList[0];
  const last = csList[csList.length - 1];
  ss.sum = last.value - first.value;
  const spanMs = last.ts - first.ts;
  if (spanMs > 0) ss.tps = (ss.sum * 1000.0) / spanMs;
  const timesDiff = last.times - first.times;
  ss.times = timesDiff;
  if (timesDiff > 0) ss.avgpt = (ss.sum * 1.0) / timesDiff;
  return ss;
}

export class StatsItem {
  statsName: string;
  statsKey: string;
  private _value = 0;
  private _times = 0;
  // Entries = (timestamp_ms, cumulative value, cumulative times).
  private _minute: SamplePoint[] = [];
  private _hour: SamplePoint[] = [];

  constructor(statsName: string, statsKey: string) {
    this.statsName = statsName;
    this.statsKey = statsKey;
  }

  addValue(incValue: number, incTimes: number): void {
    this._value += incValue;
    this._times += incTimes;
  }

  get value(): number { return this._value; }
  get times(): number { return this._times; }

  private _samplePoint(): SamplePoint {
    return { ts: Date.now(), value: this._value, times: this._times };
  }

  // Append a minute-level sample point (every 10s from the sampler timer).
  sample(): void {
    this._minute.push(this._samplePoint());
    while (this._minute.length > MINUTE_LIST_MAX) this._minute.shift();
  }

  // Append an hour-level sample point (every 10 minutes from the sampler).
  sampleHour(): void {
    this._hour.push(this._samplePoint());
    while (this._hour.length > HOUR_LIST_MAX) this._hour.shift();
  }

  getStatsDataInMinute(): StatsSnapshot {
    return computeStatsData(this._minute);
  }

  getStatsDataInHour(): StatsSnapshot {
    return computeStatsData(this._hour);
  }
}

export class StatsItemSet {
  statsName: string;
  private _items = new Map<string, StatsItem>();

  constructor(statsName: string) {
    this.statsName = statsName;
  }

  getAndCreate(key: string): StatsItem {
    let item = this._items.get(key);
    if (item == null) {
      item = new StatsItem(this.statsName, key);
      this._items.set(key, item);
    }
    return item;
  }

  find(key: string): StatsItem | null {
    return this._items.get(key) != null ? this._items.get(key)! : null;
  }

  addValue(key: string, incValue: number, incTimes: number): void {
    this.getAndCreate(key).addValue(incValue, incTimes);
  }

  keys(): string[] {
    return Array.from(this._items.keys());
  }

  sampleAll(): void {
    for (const key of this.keys()) this.find(key)!.sample();
  }

  sampleHourAll(): void {
    for (const key of this.keys()) this.find(key)!.sampleHour();
  }
}

export class ConsumerStatsManager {
  // Five StatsItemSets; the key is ALWAYS `topic@group`.
  topicAndGroupPullRT = new StatsItemSet('PULL_RT');
  topicAndGroupPullTPS = new StatsItemSet('PULL_TPS');
  topicAndGroupConsumeRT = new StatsItemSet('CONSUME_RT');
  topicAndGroupConsumeOKTPS = new StatsItemSet('CONSUME_OK_TPS');
  topicAndGroupConsumeFailedTPS = new StatsItemSet('CONSUME_FAILED_TPS');

  private _sets: StatsItemSet[];
  private _timer: any = null;
  private _rounds = 0;

  constructor() {
    this._sets = [
      this.topicAndGroupPullRT,
      this.topicAndGroupPullTPS,
      this.topicAndGroupConsumeRT,
      this.topicAndGroupConsumeOKTPS,
      this.topicAndGroupConsumeFailedTPS,
    ];
  }

  // start 本身只做定时器；采样精度保持 10s。
  start(): void {
    if (this._timer != null) return;
    this._timer = setInterval(() => {
      try {
        this._rounds += 1;
        for (const s of this._sets) s.sampleAll();
        if (this._rounds % 60 === 0) { // 60 × 10s = 10 minutes
          for (const s of this._sets) s.sampleHourAll();
        }
      } catch (e) {
        logger.warn(`stats sample failed: ${(e as Error).message}`);
      }
    }, SAMPLING_INTERVAL_SECONDS * 1000);
    // Never keep the process alive for a stats timer.
    if (typeof this._timer.unref === 'function') this._timer.unref();
  }

  shutdown(): void {
    if (this._timer != null) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  private static _key(topic: string, group: string): string {
    return `${topic}@${group}`;
  }

  // ---- 记录接口：incPullRT / incPullTPS / incConsumeRT / incConsumeOKTPS / incConsumeFailedTPS ----
  incPullRT(group: string, topic: string, rt: number): void {
    this.topicAndGroupPullRT.addValue(ConsumerStatsManager._key(topic, group), rt, 1);
  }

  incPullTPS(group: string, topic: string, msgs: number): void {
    this.topicAndGroupPullTPS.addValue(ConsumerStatsManager._key(topic, group), msgs, 1);
  }

  incConsumeRT(group: string, topic: string, rt: number): void {
    this.topicAndGroupConsumeRT.addValue(ConsumerStatsManager._key(topic, group), rt, 1);
  }

  incConsumeOKTPS(group: string, topic: string, msgs: number): void {
    this.topicAndGroupConsumeOKTPS.addValue(ConsumerStatsManager._key(topic, group), msgs, 1);
  }

  incConsumeFailedTPS(group: string, topic: string, msgs: number): void {
    this.topicAndGroupConsumeFailedTPS.addValue(ConsumerStatsManager._key(topic, group), msgs, 1);
  }

  // consumeStatus：除 consumeFailedMsgs 外全部走 MINUTE 快照；consumeFailedMsgs
  // 取失败集合的 HOUR 窗口 sum ——跨窗口是刻意设计，别自作主张改。
  consumeStatus(group: string, topic: string): ConsumeStatus {
    const cs = new ConsumeStatus();
    const key = ConsumerStatsManager._key(topic, group);
    const pullRT = this.topicAndGroupPullRT.find(key);
    if (pullRT != null) cs.pullRT = pullRT.getStatsDataInMinute().avgpt;
    const pullTPS = this.topicAndGroupPullTPS.find(key);
    if (pullTPS != null) cs.pullTPS = pullTPS.getStatsDataInMinute().tps;
    const consumeRT = this.topicAndGroupConsumeRT.find(key);
    if (consumeRT != null) cs.consumeRT = consumeRT.getStatsDataInMinute().avgpt;
    const okTPS = this.topicAndGroupConsumeOKTPS.find(key);
    if (okTPS != null) cs.consumeOKTPS = okTPS.getStatsDataInMinute().tps;
    const failedTPS = this.topicAndGroupConsumeFailedTPS.find(key);
    if (failedTPS != null) {
      cs.consumeFailedTPS = failedTPS.getStatsDataInMinute().tps;
      cs.consumeFailedMsgs = failedTPS.getStatsDataInHour().sum;
    }
    return cs;
  }
}
