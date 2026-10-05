// -*- coding: utf-8 -*-
// Consumer-side statistics (org.apache.rocketmq.client.stat.ConsumerStatsManager
// and org.apache.rocketmq.common.stats.{StatsItem, StatsItemSet, StatsSnapshot}).
// Faithful port of python/client/consumer_stats.py, which was verified
// against the Java 5.5.1 sources line by line.
//
// The REAL Java model (NOT "one bucket per minute" as the name suggests):
//
// * A StatsItem holds CUMULATIVE value/times (only ever increase), plus TWO
//   sampling snapshot chains: `csListMinute` (a cumulative point every 10s)
//   and `csListHour` (a cumulative point every 10 minutes);
// * Snapshot computation is Java StatsItem.computeStatsData:
//       sum       = last.value - first.value          (window delta)
//       tps       = sum * 1000.0 / (last.ts - first.ts)  (per SECOND)
//       timesDiff = last.times - first.times
//       avgpt     = timesDiff > 0 ? sum / timesDiff : 0  ("avg per time" —
//                   for RT items this IS the average latency in ms)
// * TPS-style counters use addValue(key, msgs, 1): value accumulates MESSAGE
//   counts, times accumulates CALL counts → tps = calls (or msgs) per second;
// * RT-style counters use addRTValue(key, rt, 1): value accumulates elapsed
//   ms, times accumulates calls → avgpt = average latency;
// * consumeStatus(group, topic) reads MINUTE snapshots for everything EXCEPT
//   consumeFailedMsgs, which takes the failed set's HOUR-window sum (Java
//   deliberately crosses windows — replicate, don't "fix").
//
// Implementation difference (semantics preserved): Java schedules a 10s/10min
// sampler per StatsItem; here ONE manager-level sampling timer walks all sets
// — identical sampling precision (10s), one timer instead of dozens.
import { ConsumeStatus } from '../remoting/bodies.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('consumer-stats');

// Java StatsItem.init's scheduleAtFixedRate parameters.
export const SAMPLING_INTERVAL_SECONDS = 10.0;
export const HOUR_SAMPLING_INTERVAL_SECONDS = 600.0;
// Snapshot chain length (Java csListMinute caps at ~60 points ≈ 10min window).
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

// Java StatsItem.computeStatsData, line by line.
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

  // Java's start() is empty (sampling hangs off each StatsItem's scheduler);
  // here ONE unified sampler timer — precision unchanged (10s).
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

  // ---- recording (Java ConsumerStatsManager's same-named methods) ----
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

  // Java ConsumerStatsManager.consumeStatus: everything from MINUTE snapshots;
  // consumeFailedMsgs from the failed set's HOUR-window sum (Java deliberately
  // crosses windows — replicate).
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
