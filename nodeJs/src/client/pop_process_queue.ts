// -*- coding: utf-8 -*-
// PopProcessQueue — port of Java
// org.apache.rocketmq.client.impl.consumer.PopProcessQueue (mirrors
// go/client/pop_process_queue.go).
//
// It is deliberately NOT the ordinary ProcessQueue: POP has no offset cursor
// and no message buffer. The broker hands out an invisible batch and expects an
// ACK (or an invisibility extension) per message; all the client tracks is how
// many it still owes an answer for. That counter is what flow control reads.
//
// The one trap in the Java class: `ack()` returns the value BEFORE
// decrementing, while `decFoundMsg()` ADDS (not subtracts) — it is a "give the
// messages back" undo path. Both are reproduced with the Java behaviour,
// because ConsumeMessagePopConcurrentlyService relies on the sign.
import { currentTimeMillis } from '../common/utilAll.ts';

// Java's PULL_MAX_IDLE_TIME (overridable via ROCKETMQ_CLIENT_PULL_MAX_IDLE_TIME).
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

  // ack mirrors Java ack(): returns the value BEFORE the decrement
  // (AtomicInteger.getAndDecrement). The live assertions read the pre-value.
  ack(): number { return this.waitAckCounter--; }

  // decFoundMsg mirrors Java decFoundMsg, which ADDS. It is the "abort this
  // batch, hand the debt back" path used when a batch turns out to be
  // undeliverable (dropped queue / popped out of its invisibility window).
  // Callers pass a negative count to return the debt: decFoundMsg(-msgs.length).
  decFoundMsg(count: number): void { this.waitAckCounter += count; }

  // waitAckMsgCount is the outstanding-ACK count (Java's misspelled
  // getWaiAckMsgCount).
  waitAckMsgCount(): number { return this.waitAckCounter; }

  isDropped(): boolean { return this.dropped; }
  setDropped(dropped = true): void { this.dropped = dropped; }

  // isPullExpired mirrors Java isPullExpired. The POP rebalance uses it to
  // retire a queue whose pop loop has gone quiet (the broker would otherwise
  // keep reviving its batch).
  isPullExpired(): boolean {
    return currentTimeMillis() - this.lastPopTimestamp > POP_PULL_MAX_IDLE_TIME;
  }
}
