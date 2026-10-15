// -*- coding: utf-8 -*-
// Consumer-side stats (ConsumerStatsManager + StatsItem/StatsItemSet/StatsSnapshot).
//
// The real model (NOT what the "one bucket per minute" name suggests):
//
// * A StatsItem maintains CUMULATIVE value/times (monotonically increasing)
//   with two sample-snapshot chains attached: `csListMinute` (one cumulative
//   point every 10s) and `csListHour` (one cumulative point every 10 minutes);
// * Snapshots are computed by computeStatsData:
//       sum       = last.value - first.value          (window difference)
//       tps       = sum * 1000.0 / (last.ts - first.ts)  (per second)
//       timesDiff = last.times - first.times
//       avgpt     = timesDiff > 0 ? sum / timesDiff : 0  ("per call") — on RT
//                   metrics this is the average latency (ms)
// * TPS counters go through addValue(key, msgs, 1): value accumulates the
//   message count, times accumulates the call count →
//   tps = calls (or messages) per second;
// * RT counters go through addRTValue(key, rt, 1): value accumulates elapsed
//   ms, times accumulates the call count → avgpt = average latency;
// * consumeStatus(group, topic) reads the MINUTE snapshot for everything
//   except consumeFailedMsgs; consumeFailedMsgs takes the failed set's
//   HOUR-window sum — a DELIBERATE cross-window choice, do not "fix" it in
//   passing.
//
// Implementation detail (semantics identical): originally each StatsItem had
// its own 10s/10min sampler; here ONE manager-level sampling timer walks every
// set uniformly — the sampling precision is unchanged (10s) while dozens of
// timers are saved.
import { ConsumeStatus } from '../remoting/bodies.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('consumer-stats');

// Sampling interval: minute takes a point every 10s, hour every 10min.
export const SAMPLING_INTERVAL_SECONDS = 10.0;
export const HOUR_SAMPLING_INTERVAL_SECONDS = 600.0;
// Snapshot-chain length cap: about 60 points ≈ a 10-minute window (minute chain).
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

// Snapshot computation: window first-vs-last difference -> sum / tps / timesDiff / avgpt.
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

  // start itself only sets up the timer; the sampling precision stays 10s.
  start(): void {
    if (this._timer != null) return;
    this._timer = setInterval(() => {
      try {
        this._rounds += 1;
        for (const s of this._sets) s.sampleAll();
        if (this._rounds % 60 === 0) { // 60 x 10s = 10 minutes
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

  // ---- recording interface: incPullRT / incPullTPS / incConsumeRT / incConsumeOKTPS / incConsumeFailedTPS ----
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

  // consumeStatus: everything except consumeFailedMsgs uses the MINUTE
  // snapshot; consumeFailedMsgs takes the failed set's HOUR-window sum — the
  // cross-window is deliberate, do not change it unilaterally.
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
