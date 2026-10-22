// The async-send backpressure gate: two fair semaphores bound how many async sends, and how
// many in-flight bytes, the producer has outstanding at once. They are only consulted while
// enableBackpressureForAsyncMode is on.
//
// A request that finds no permits free waits for them until its own budget runs out, and then
// reports the refusal to its caller. Only the head of the line may be granted, so a request
// never starves behind a larger one that keeps jumping the queue.
//
// Waiting is only fair if a refusal is final. A wake-up can be delivered while the caller still
// has budget and taken after that budget is gone, so every pass after the first re-checks the
// deadline before granting: handing a permit to a caller that had already given up would leave
// it holding capacity the next waiter could still have used in time.
//
// A non-positive budget is a single poll: granted when permits are free right now, refused
// otherwise -- it never waits.

export const MIN_ASYNC_SEND_NUM = 10;
export const MIN_ASYNC_SEND_SIZE = 1024 * 1024;

interface Waiter {
  permits: number;
  deadline: number;
  resolve: (granted: boolean) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class FairSemaphore {
  totalPermits: number;
  availablePermits: number;
  private readonly _waiters: Waiter[] = [];

  constructor(totalPermits: number = 1024) {
    this.totalPermits = totalPermits;
    this.availablePermits = totalPermits;
  }

  tryAcquire(permits: number = 1, timeoutMillis: number = 0): Promise<boolean> {
    const deadline = Date.now() + Math.max(timeoutMillis, 0);
    if (this._waiters.length === 0) {
      if (this.availablePermits >= permits) {
        this.availablePermits -= permits;
        return Promise.resolve(true);
      }
      if (timeoutMillis <= 0) return Promise.resolve(false);
    } else if (timeoutMillis <= 0) {
      return Promise.resolve(false);
    }
    let settle: (granted: boolean) => void = () => {};
    const answered = new Promise<boolean>((resolve) => { settle = resolve; });
    const waiter: Waiter = {
      permits,
      deadline,
      resolve: (granted: boolean) => settle(granted),
      timer: setTimeout(() => { this._drop(waiter); this._dispatch(); }, Math.max(deadline - Date.now(), 1)),
    };
    this._waiters.push(waiter);
    return answered;
  }

  release(permits: number = 1): void {
    if (permits <= 0) return;
    this.availablePermits += permits;
    this._dispatch();
  }

  availablePermitsCount(): number { return this.availablePermits; }
  totalPermitsCount(): number { return this.totalPermits; }

  // Requests still in line. Only a caller that gave up on the spot leaves none behind, so this
  // is the observable half of "a refusal is final".
  waitingCount(): number { return this._waiters.length; }

  setTotalPermits(permits: number): void {
    // The free counter moves by the delta, so shrinking below what is already in flight gives a
    // negative margin that the gate refuses to grant over until that traffic drains.
    this.availablePermits += permits - this.totalPermits;
    this.totalPermits = permits;
    this._dispatch();
  }

  private _dispatch(): void {
    while (this._waiters.length > 0) {
      const head = this._waiters[0];
      if (head.deadline - Date.now() <= 0) {
        this._drop(head);
        continue;
      }
      if (this.availablePermits < head.permits) return;
      this._grant(head);
    }
  }

  private _grant(head: Waiter): void {
    this._waiters.shift();
    clearTimeout(head.timer);
    this.availablePermits -= head.permits;
    head.resolve(true);
  }

  private _drop(head: Waiter): void {
    const at = this._waiters.indexOf(head);
    if (at < 0) return;
    this._waiters.splice(at, 1);
    clearTimeout(head.timer);
    head.resolve(false);
  }
}

export default { FairSemaphore, MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE };
