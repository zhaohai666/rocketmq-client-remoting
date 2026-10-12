// -*- coding: utf-8 -*-
// PopProcessQueue —— POP 消费下的队列状态。
//
// 它刻意**不是**普通 ProcessQueue：POP 既没有 offset 游标，也不缓冲消息。broker
// 交出一批不可见消息，客户端需要按条 ACK 或续不可见期，客户端只跟踪"还欠多少条回复"，
// 这个计数就是流控读取的量。
//
// 这个类里有两个坑：`ack()` 返回的是**减一之前**的值；`decFoundMsg()` 做的是**加**
// 而不是减——它是"这一批撤回，把债再挂回去"的路径。两处都得原样保留，因为
// ConsumeMessagePopConcurrentlyService 依赖这个符号方向。
import { currentTimeMillis } from '../common/utilAll.ts';

// POP 复用的空闲上限（默认 120000ms，可用 ROCKETMQ_CLIENT_PULL_MAX_IDLE_TIME 覆盖）。
const POP_PULL_MAX_IDLE_TIME = (() => {
  const raw = process.env.ROCKETMQ_CLIENT_PULL_MAX_IDLE_TIME;
  if (!raw) return 120000;
  const v = parseInt(raw, 10);
  return Number.isFinite(v) && v > 0 ? v : 120000;
})();

export class PopProcessQueue {
  lastPopTimestamp: number;
  waitAckCounter = 0;
  dropped = false;

  constructor() {
    this.lastPopTimestamp = currentTimeMillis();
  }

  getLastPopTimestamp(): number { return this.lastPopTimestamp; }
  setLastPopTimestamp(ts: number): void { this.lastPopTimestamp = ts; }

  // incFoundMsg: the broker just made this many messages invisible and we owe
  // an answer for each.
  incFoundMsg(count: number): void { this.waitAckCounter += count; }

  // ack()：返回**减一之前**的原值（getAndDecrement 语义）；实时刻度读的是自减前的量。
  ack(): number { return this.waitAckCounter--; }

  // decFoundMsg 做的是**加**：这是"整批撤回，把欠回复重新挂回来"的路径，用于队列
  // 已 dropped 或消息已经出了不可见窗口这种不可投递场景。调用方传负数来销账：
  // decFoundMsg(-msgs.length)。
  decFoundMsg(count: number): void { this.waitAckCounter += count; }

  // 未回 ACK 的条数。
  waitAckMsgCount(): number { return this.waitAckCounter; }

  isDropped(): boolean { return this.dropped; }
  setDropped(dropped = true): void { this.dropped = dropped; }

  // POP rebalance 用它淘汰 pop 循环静默太久的队列（不然 broker 会一直 revival 它的批次）。
  isPullExpired(): boolean {
    return currentTimeMillis() - this.lastPopTimestamp > POP_PULL_MAX_IDLE_TIME;
  }
}
