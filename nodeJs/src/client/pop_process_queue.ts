// -*- coding: utf-8 -*-
// PopProcessQueue — queue state under POP consumption.
//
// It is deliberately NOT a normal ProcessQueue: POP has neither an offset
// cursor nor message buffering. The broker hands out a batch of invisible
// messages; the client must ACK each one or extend the invisibility period, and
// the client only tracks "how many replies are still owed", and that count is
// the amount read for flow control.
//
// Two traps in this class: `ack()` returns the value BEFORE the decrement;
// `decFoundMsg()` ADDS instead of subtracts — it is the "this batch is
// withdrawn, hang the debt back on" path. Both must be preserved as-is, because
// ConsumeMessagePopConcurrentlyService depends on this sign direction.
import { currentTimeMillis } from '../common/utilAll.ts';

// Idle cap reused by POP (default 120000ms, overridable via ROCKETMQ_CLIENT_PULL_MAX_IDLE_TIME).
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

  // ack(): returns the original value BEFORE the decrement (getAndDecrement semantics); the instant metric reads the pre-decrement amount.
  ack(): number { return this.waitAckCounter--; }

  // decFoundMsg ADDS: this is the "whole batch withdrawn, hang the owed replies
  // back on" path, used when the queue has already been dropped or the messages
  // have left the invisibility window — undeliverable scenarios. The caller
  // passes a negative number to write the debt off: decFoundMsg(-msgs.length).
  decFoundMsg(count: number): void { this.waitAckCounter += count; }

  // The number of messages not yet ACKed.
  waitAckMsgCount(): number { return this.waitAckCounter; }

  isDropped(): boolean { return this.dropped; }
  setDropped(dropped = true): void { this.dropped = dropped; }

  // POP rebalance uses this to evict queues whose pop loop has been silent too
  // long (otherwise the broker would keep reviving their batches).
  isPullExpired(): boolean {
    return currentTimeMillis() - this.lastPopTimestamp > POP_PULL_MAX_IDLE_TIME;
  }
}
