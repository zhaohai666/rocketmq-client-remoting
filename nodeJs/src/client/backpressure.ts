// -*- coding: utf-8 -*-
// Fair semaphore for async-send backpressure (org.apache.rocketmq.client.producer.backpressure).
// Faithful port of python/client/backpressure.py.
//
// A fair semaphore bounds the number of in-flight async sends. When the free permits drop below
// MIN_ASYNC_SEND_NUM (or the pending byte size below MIN_ASYNC_SEND_SIZE), the client falls back
// to synchronous sending to avoid unbounded queueing.

export const MIN_ASYNC_SEND_NUM = 10;
export const MIN_ASYNC_SEND_SIZE = 1024 * 1024;

export class FairSemaphore {
  totalPermits: number;
  availablePermits: number;

  constructor(totalPermits: number = 1024) {
    this.totalPermits = totalPermits;
    this.availablePermits = totalPermits;
  }

  tryAcquire(): boolean {
    if (this.availablePermits > 0) {
      this.availablePermits--;
      return true;
    }
    return false;
  }

  // Acquire `n` permits at once (byte-size backpressure). Single-threaded
  // event loop: the check-and-decrement is effectively atomic.
  tryAcquireFor(n: number): boolean {
    if (this.availablePermits >= n) {
      this.availablePermits -= n;
      return true;
    }
    return false;
  }

  release(): void {
    if (this.availablePermits < this.totalPermits) {
      this.availablePermits++;
    }
  }

  releaseFor(n: number): void {
    this.availablePermits = Math.min(this.totalPermits, this.availablePermits + n);
  }

  setTotalPermits(permits: number): void {
    this.totalPermits = permits;
    if (this.availablePermits > permits) this.availablePermits = permits;
  }

  availablePermitsCount(): number { return this.availablePermits; }
  totalPermitsCount(): number { return this.totalPermits; }
}

export default { FairSemaphore, MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE };
