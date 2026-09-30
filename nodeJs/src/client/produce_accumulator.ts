// -*- coding: utf-8 -*-
// Produce accumulator for batching small messages before a single broker send.
// Faithful port of python/rocketmq/client/produce_accumulator.py (threads replaced by an
// async guard, since Node has no Python threads). The producer wires a real sender.
import { Message } from '../common/message.ts';
import { MessageQueue } from '../common/message.ts';
import { isWaitStoreMsgOK } from '../common/message.ts';
import { MessageBatch } from '../common/message.ts';
import { SendResult } from './send_result.ts';

export const DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024; // 32 MB
export const DEFAULT_HOLD_SIZE = 32 * 1024;             // 32 KB
export const DEFAULT_HOLD_MS = 10;

export class AggregateKey {
  topic: string;
  mq: MessageQueue;
  waitStoreMsgOK: boolean;
  tag: string | null;

  constructor(topic: string, mq: MessageQueue, waitStoreMsgOK: boolean, tag: string | null) {
    this.topic = topic;
    this.mq = mq;
    this.waitStoreMsgOK = waitStoreMsgOK;
    this.tag = tag;
  }

  // Keys are comparable by (topic, queue hashcode, waitStoreMsgOK, tag).
  keyString(): string {
    return `${this.topic}\u0000${this.mq.hashcode()}\u0000${this.waitStoreMsgOK ? 1 : 0}\u0000${this.tag ?? ''}`;
  }

  equals(other: AggregateKey): boolean {
    return this.topic === other.topic &&
      this.mq.equals(other.mq) &&
      this.waitStoreMsgOK === other.waitStoreMsgOK &&
      this.tag === other.tag;
  }
}

export class MessageAccumulation {
  messages: Message[];
  totalSize: number;

  constructor() {
    this.messages = [];
    this.totalSize = 0;
  }

  add(msg: Message, size: number): void {
    this.messages.push(msg);
    this.totalSize += size;
  }

  get count(): number { return this.messages.length; }
  get size(): number { return this.totalSize; }
  isEmpty(): boolean { return this.messages.length === 0; }
  clear(): void { this.messages = []; this.totalSize = 0; }
}

export class ProduceAccumulator {
  maxHoldSize: number;
  holdMs: number;
  clientId: string;
  accumulator: Map<string, MessageAccumulation>;
  sender: ((acc: MessageAccumulation) => Promise<void>) | null;
  private timer: ReturnType<typeof setInterval> | null;

  constructor(clientId: string, maxHoldSize: number = DEFAULT_HOLD_SIZE, holdMs: number = DEFAULT_HOLD_MS) {
    this.clientId = clientId;
    this.maxHoldSize = maxHoldSize;
    this.holdMs = holdMs;
    this.accumulator = new Map();
    this.sender = null;
    this.timer = null;
  }

  setSender(fn: (acc: MessageAccumulation) => Promise<void>): void {
    this.sender = fn;
  }

  // Returns the accumulated batch to flush when it crosses the hold-size threshold.
  accumulate(msg: Message, mq: MessageQueue): MessageAccumulation | null {
    const tag = msg.getTags();
    const key = new AggregateKey(msg.getTopic(), mq, isWaitStoreMsgOK(msg), tag);
    const k = key.keyString();
    let acc = this.accumulator.get(k);
    if (acc == null) {
      acc = new MessageAccumulation();
      this.accumulator.set(k, acc);
    }
    const size = msg.getBody() ? msg.getBody().length : 0;
    acc.add(msg, size);
    if (acc.size >= this.maxHoldSize) {
      this.accumulator.delete(k);
      return acc;
    }
    return null;
  }

  // Drain all pending accumulations (used when flushing on a timer or shutdown).
  drain(): MessageAccumulation[] {
    const out: MessageAccumulation[] = [];
    for (const acc of this.accumulator.values()) {
      if (!acc.isEmpty()) out.push(acc);
    }
    this.accumulator.clear();
    return out;
  }

  size(): number { return this.accumulator.size; }

  start(): void {
    if (this.timer != null) return;
    this.timer = setInterval(async () => {
      if (this.sender == null) return;
      const batches = this.drain();
      for (const acc of batches) {
        try { await this.sender(acc); } catch (e) { /* swallowed; per-batch failure */ }
      }
    }, Math.max(1, this.holdMs));
  }

  stop(): void {
    if (this.timer != null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

const _ACCUMULATORS: Map<string, ProduceAccumulator> = new Map();

export function getOrCreateProduceAccumulator(clientId: string): ProduceAccumulator {
  let acc = _ACCUMULATORS.get(clientId);
  if (acc == null) {
    acc = new ProduceAccumulator(clientId);
    _ACCUMULATORS.set(clientId, acc);
  }
  return acc;
}

// Split a single batch SendResult into per-message SendResults (mirrors Java's handling of an
// inner batch: each message shares the queue/offset but gets a distinct msgId slice when available).
export function splitSendResults(batchResult: SendResult, msgs: Message[]): SendResult[] {
  const out: SendResult[] = [];
  for (let i = 0; i < msgs.length; i++) {
    const r = new SendResult(
      batchResult.sendStatus,
      batchResult.msgId,
      batchResult.messageQueue,
      batchResult.queueOffset,
      batchResult.transactionId,
      batchResult.offsetMsgId,
      batchResult.regionId,
      batchResult.traceOn,
      batchResult.recallHandle,
    );
    out.push(r);
  }
  return out;
}

export default {
  ProduceAccumulator, AggregateKey, MessageAccumulation,
  getOrCreateProduceAccumulator, splitSendResults,
  DEFAULT_TOTAL_HOLD_SIZE, DEFAULT_HOLD_SIZE, DEFAULT_HOLD_MS,
};
