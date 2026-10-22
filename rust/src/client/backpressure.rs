//! * `semaphoreAsyncSendNum` -- in-flight **message count**, `back_pressure_for_async_send_num` defaults to 1024,
//!   floor 10;
//! * `semaphoreAsyncSendSize` -- in-flight **byte count**, `back_pressure_for_async_send_size` defaults to 100M,
//!   floor 1M, one message deducts `body.len()` permits (an empty body counts as 1).
//!
//! Both permits are waited for with the **entire remaining budget**; if the wait fails the callback is
//! `Error::TooMuchRequest("send message tryAcquire semaphoreAsyncNum timeout")`
//! (the second one is `...semaphoreAsyncSize timeout`), and not a single request goes out.
//!
//! # Two design trade-offs in FairSemaphore

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::rmq_info;

pub const MIN_ASYNC_SEND_NUM: i64 = 10;
pub const MIN_ASYNC_SEND_SIZE: i64 = 1024 * 1024;

/// A permit request still in the queue (exists only so the head can be identified stably; it does not mean the permit is held).
///
/// `permits` is deliberately not stored here: every time a waiter wakes it brings its own requested amount to
/// inspect the queue (the input of `grant_if_head`), so a ticket only needs to be recognizable.
struct Pending {
    id: u64,
}

struct State {
    total: i64,
    free: i64,
    queue: VecDeque<Pending>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// * no extra spin rwlock layer is needed -- resizing capacity and taking/releasing permits are mutually exclusive inside the object;
/// * a capacity change never strands waiters on the old object; after the change they keep waiting under the new capacity.
pub struct FairSemaphore {
    state: Mutex<State>,
    /// Condition-variable style wakeup: "broadcast" = `notify_waiters`,
    /// each waiter re-inspects the queue itself after being woken (only the head can pass).
    wake: tokio::sync::Notify,
    next_id: AtomicU64,
}

impl FairSemaphore {
    pub fn new(permits: i64) -> FairSemaphore {
        FairSemaphore {
            state: Mutex::new(State {
                total: permits,
                free: permits,
                queue: VecDeque::new(),
            }),
            wake: tokio::sync::Notify::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// ⚠ Both exits -- taking the permit and **abandoning the queue** -- must notify once more: in fair mode only the head can take,
    /// once the head changes, a later request can flip from "not my turn" to "my turn", and its `permits` amount
    /// was not necessarily affected by the previous holder's action (head wants 5 with 6 free: the head takes 5 leaving 1,
    /// exactly enough for the second waiter wanting 1 -- but `release` had already run, nobody wakes it for that).
    /// Miss this one wakeup and that waiter sleeps until its own timeout: in the field a 5-second dead wait, not a dropped message.
    pub async fn try_acquire(&self, permits: i64, timeout_millis: i64) -> bool {
        let budget = u64::try_from(timeout_millis.max(0)).unwrap_or(u64::MAX);
        let deadline = Instant::now() + Duration::from_millis(budget);
        let _ticket = Ticket::new(self);
        loop {
            // Register the wake-up credential first, then inspect the queue: `notify_waiters` only wakes those already waiting at that moment
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.grant_if_head(_ticket.id, permits) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Budget exhausted: the ticket removes itself from the queue on Drop so it does not block those behind
                return false;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return false;
            }
            // Woken by a release or a resize. On a loaded host this task can be polled long after both the wake-up
            // and the budget, and `Timeout` reports the inner future first -- so a wake that arrived before the
            // deadline still lands here after it. The budget is what the caller asked for: taking the permit now
            // would let an already-expired send cut ahead of the queue it just left, then hand the permit back a
            // few statements later. Drop the ticket (`Ticket::Drop` wakes the next waiter) and leave it to someone
            // still inside their own deadline.
            if Instant::now() >= deadline {
                return false;
            }
            // Woken: loop back to the top and re-inspect the queue (possibly a spurious wake; re-inspecting just sleeps again)
        }
    }

    /// The permit is granted only when the caller is the head and the free permits suffice; on grant it also wakes those behind.
    fn grant_if_head(&self, id: u64, permits: i64) -> bool {
        let granted = {
            let mut state = lock(&self.state);
            let at_head = state
                .queue
                .front()
                .is_some_and(|pending| pending.id == id);
            if at_head && state.free >= permits {
                state.queue.pop_front();
                state.free -= permits;
                true
            } else {
                false
            }
        };
        if granted {
            self.wake.notify_waiters();
        }
        granted
    }

    pub fn release(&self, permits: i64) {
        if permits <= 0 {
            return;
        }
        {
            lock(&self.state).free += permits;
        }
        self.wake.notify_waiters();
    }

    /// Currently free permits (may be negative, see [`set_total_permits`](Self::set_total_permits)).
    pub fn available_permits(&self) -> i64 {
        lock(&self.state).free
    }

    /// Current total capacity.
    pub fn total_permits(&self) -> i64 {
        lock(&self.state).total
    }

    pub fn waiting_count(&self) -> usize {
        lock(&self.state).queue.len()
    }

    pub fn set_total_permits(&self, total: i64) {
        {
            let mut state = lock(&self.state);
            state.free += total - state.total;
            state.total = total;
        }
        self.wake.notify_waiters();
    }
}

/// Queuing ticket: `Drop` removes the waiter from the queue -- when it did **not** get the permit -- and wakes those behind.
///
/// When the permit was granted the ticket was already popped by `grant_if_head`, so Drop finds nothing -- no extra
/// "already granted" flag is needed, hence bugs like "forgot to disarm and emptied the head slot" cannot exist.
struct Ticket<'a> {
    sem: &'a FairSemaphore,
    id: u64,
}

impl<'a> Ticket<'a> {
    fn new(sem: &'a FairSemaphore) -> Ticket<'a> {
        let id = sem.next_id.fetch_add(1, Ordering::AcqRel);
        lock(&sem.state).queue.push_back(Pending { id });
        Ticket { sem, id }
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let removed = {
            let mut state = lock(&self.sem.state);
            match state.queue.iter().position(|pending| pending.id == self.id) {
                Some(pos) => {
                    state.queue.remove(pos);
                    true
                }
                None => false,
            }
        };
        if removed {
            // The blocker is gone: the head changed, and the next waiter may already have enough
            self.sem.wake.notify_waiters();
        }
    }
}

pub fn back_pressure_permits(configured: i64, floor: i64, name: &str) -> i64 {
    if configured > floor {
        return configured;
    }
    rmq_info!("{name} can not be smaller than {floor}.");
    floor
}

#[cfg(test)]
mod tests {
    use std::time::Instant;
    use std::sync::Arc;

    use super::*;

    fn semaphore(permits: i64) -> Arc<FairSemaphore> {
        Arc::new(FairSemaphore::new(permits))
    }

    /// Poll until the condition holds (roughly 1s at most).
    ///
    /// `yield_now` alone does not work: on a single-threaded runtime it yields scheduling but never advances time, so a few
    /// tasks waiting for each other to enqueue their tickets first spin into a deadlock (the first version of this module died right here).
    async fn wait_until<F: Fn() -> bool>(cond: F) -> bool {
        for _ in 0..500 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        cond()
    }

    /// Wait until the queue holds exactly `n` tickets -- the fairness cases rely on it to pin the arrival order.
    async fn wait_queue_len(sem: &FairSemaphore, n: usize) {
        assert!(
            wait_until(|| sem.waiting_count() == n).await,
            "applicant #{n} never joined the queue (currently {})",
            sem.waiting_count()
        );
    }

    /// Runs one `try_acquire` in the background, returning whether it got the permit and how long it waited (real clock).
    fn acquire_in_background(
        sem: &Arc<FairSemaphore>,
        permits: i64,
        budget: i64,
    ) -> tokio::task::JoinHandle<(bool, Duration)> {
        let sem = sem.clone();
        tokio::spawn(async move {
            let began = Instant::now();
            let got = sem.try_acquire(permits, budget).await;
            (got, began.elapsed())
        })
    }

    #[tokio::test]
    async fn acquires_and_releases_in_contract_units() {
        let sem = semaphore(3);
        assert_eq!(sem.available_permits(), 3);
        assert!(sem.try_acquire(2, 100).await);
        assert_eq!(sem.available_permits(), 1);
        sem.release(2);
        assert_eq!(sem.available_permits(), 3);
        sem.release(5);
        assert_eq!(sem.available_permits(), 8);
        assert_eq!(sem.total_permits(), 3);
    }

    #[tokio::test]
    async fn zero_permit_requests_are_ignored() {
        let sem = semaphore(2);
        sem.release(0);
        sem.release(-1);
        assert_eq!(sem.available_permits(), 2);
    }

    #[tokio::test]
    async fn only_the_head_may_take_permits() {
        let sem = semaphore(2);
        assert!(sem.try_acquire(2, 0).await);
        assert_eq!(sem.available_permits(), 0);

        let late = acquire_in_background(&sem, 1, 50);
        // Wait until late has really queued up, otherwise the verdict below is just luck
        wait_queue_len(&sem, 1).await;
        // The head wants 1 while 0 are free: nothing can cut in
        let (got, _) = late.await.unwrap();
        assert!(!got);
        assert_eq!(sem.available_permits(), 0);
    }

    #[tokio::test]
    async fn a_blocked_head_keeps_waiting_tasks_queued_in_order() {
        let sem = semaphore(1);
        assert!(sem.try_acquire(1, 0).await);
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..3 {
            let task_sem = sem.clone();
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                let got = task_sem.try_acquire(1, 5000).await;
                if got {
                    order.lock().unwrap().push(i);
                }
                got
            }));
            // Pin the arrival order one by one: only after applicant i queues up is applicant i+1 released
            wait_queue_len(&sem, i + 1).await;
        }
        sem.release(3);
        for h in handles {
            assert!(h.await.unwrap());
        }
        // Fair = released strictly in arrival order, not "whoever is woken first wins"
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(sem.available_permits(), 0);
    }

    #[tokio::test]
    async fn resizing_keeps_outstanding_permits() {
        let sem = semaphore(10);
        assert!(sem.try_acquire(4, 0).await);
        assert_eq!(sem.available_permits(), 6);
        sem.set_total_permits(7);
        assert_eq!(sem.available_permits(), 3);
        assert_eq!(sem.total_permits(), 7);
        sem.set_total_permits(2);
        assert_eq!(sem.available_permits(), -2);
        sem.release(4);
        assert_eq!(sem.available_permits(), 2);
    }

    #[tokio::test]
    async fn a_resize_wakes_a_blocked_waiter() {
        let sem = semaphore(1);
        assert!(sem.try_acquire(1, 0).await);
        let waiting = acquire_in_background(&sem, 1, 5000);
        wait_queue_len(&sem, 1).await;
        sem.set_total_permits(2);
        let (got, _) = waiting.await.unwrap();
        assert!(got);
        assert_eq!(sem.available_permits(), 0);
    }

    /// 6 free, the head wants 5, the second wants 1: both should get their permits, and neither should wait until timeout.
    /// Guards "when the head leaves it must also wake those behind" -- `release` had long finished; if nobody
    /// shouts at the second waiter again, he plainly has enough yet could sleep the full 3000ms.
    #[tokio::test]
    async fn a_granted_head_still_feeds_the_waiter_behind_it() {
        let sem = semaphore(6);
        assert!(sem.try_acquire(5, 0).await); // 5 in flight, 1 free
        let head = acquire_in_background(&sem, 5, 3000);
        wait_queue_len(&sem, 1).await;
        // 1 free is enough for the second waiter -- but in fair mode he must wait for the head to leave first
        let second = acquire_in_background(&sem, 1, 3000);
        wait_queue_len(&sem, 2).await;
        sem.release(5); // free 1 -> 6: enough for the head right now

        let (head_got, _) = head.await.unwrap();
        let (second_got, second_waited) = second.await.unwrap();
        assert!(head_got);
        assert!(second_got, "the second waiter was left behind after the head took the permits (lost wakeup)");
        assert!(
            second_waited < Duration::from_millis(2000),
            "second waited {second_waited:?}"
        );
        assert_eq!(sem.available_permits(), 0); // 6 - 5 - 1
    }

    /// Lost-wakeup regression guard: the head wants more than the total capacity and can never get it, so it must wait out its own budget and leave --
    /// when it departs it must wake those behind, otherwise the second waiter could pass yet sleeps the full 3000ms. In the field the symptom is
    /// async sends burning the whole budget for nothing, then calling back a `semaphoreAsyncNum timeout` while the permits were free long ago.
    #[tokio::test]
    async fn a_timed_out_head_wakes_the_waiter_behind_it() {
        let sem = semaphore(3);
        assert!(sem.try_acquire(3, 0).await); // drained empty
        let head = acquire_in_background(&sem, 4, 200); // wants more than the total
        wait_queue_len(&sem, 1).await;
        let second = acquire_in_background(&sem, 1, 3000);
        wait_queue_len(&sem, 2).await;
        sem.release(3); // enough for second, but the head is the greedy one

        let (head_got, _) = head.await.unwrap();
        let (second_got, second_waited) = second.await.unwrap();
        assert!(!head_got, "asking for more than the total must not get a permit");
        assert!(second_got, "not woken after the head exited (lost wakeup)");
        assert!(
            second_waited < Duration::from_millis(2000),
            "second waited {second_waited:?}"
        );
    }

    #[tokio::test]
    async fn a_dropped_waiter_leaves_no_ghost_ticket() {
        // Cancellation safety: a waiter dropped mid-flight (task abort / losing a select!) must not jam the head forever.
        let sem = semaphore(1);
        assert!(sem.try_acquire(1, 0).await);
        let ghost = acquire_in_background(&sem, 1, 5000);
        wait_queue_len(&sem, 1).await;
        ghost.abort();
        assert!(wait_until(|| sem.waiting_count() == 0).await);

        let next = acquire_in_background(&sem, 1, 50);
        wait_queue_len(&sem, 1).await;
        sem.release(1);
        let (got, _) = next.await.unwrap();
        assert!(got, "a ghost ticket jammed the head");
    }

    /// A waiter that is granted **after** its own budget elapsed is a waiter that never should have been granted:
    /// the permit leaves the queue, sits with an expired caller for a few statements, and returns -- while a waiter
    /// still inside its deadline watched the head go by.
    ///
    /// The ordering is forced, not waited for: this runtime has a single thread, so blocking it past the waiter's
    /// budget means its timer fires and the release lands **without the task ever being polled in between**. Poll
    /// `Timeout` in that state reports the wake-up first, which is exactly the path that used to hand the permit over.
    #[tokio::test]
    async fn a_wake_that_arrives_after_the_budget_is_spent_grants_nothing() {
        let sem = semaphore(1);
        assert!(sem.try_acquire(1, 0).await);
        let expired = acquire_in_background(&sem, 1, 50);
        wait_queue_len(&sem, 1).await;

        // No poll of `expired` can happen during this: the only worker thread is busy sleeping.
        std::thread::sleep(Duration::from_millis(150));
        sem.release(1);

        let (got, waited) = expired.await.unwrap();
        assert!(
            !got,
            "a caller whose budget elapsed while it was unpoliced was still handed the permit (waited {waited:?})"
        );
        assert_eq!(
            sem.available_permits(),
            1,
            "the permit an expired caller refused must stay free for the next one"
        );
        assert_eq!(sem.waiting_count(), 0, "an abandoned ticket must not stay in the queue");
    }

    #[tokio::test]
    async fn an_expired_budget_grants_only_when_the_head_is_clear() {
        let sem = semaphore(2);
        // Budget 0 but the queue has nobody at the head and permits suffice: still granted (queue checked before the deadline)
        assert!(sem.try_acquire(1, 0).await);
        assert!(sem.try_acquire(1, 0).await);
        // Now free is 0: a 0 budget fails immediately
        assert!(!sem.try_acquire(1, 0).await);
        assert!(!sem.try_acquire(1, -5).await);
        assert_eq!(sem.waiting_count(), 0, "failing to acquire must not leave a ticket behind");
    }

    #[tokio::test]
    async fn negative_free_permits_still_drain_back_to_positive() {
        let sem = semaphore(MIN_ASYNC_SEND_NUM);
        sem.set_total_permits(1);
        assert!(sem.try_acquire(1, 0).await);
        assert_eq!(sem.available_permits(), 0);
        sem.set_total_permits(MIN_ASYNC_SEND_NUM);
        assert_eq!(sem.available_permits(), 9);
        sem.release(1);
        assert_eq!(sem.available_permits(), MIN_ASYNC_SEND_NUM);
    }

    #[tokio::test]
    async fn the_floor_values_match_contract() {
        assert_eq!(MIN_ASYNC_SEND_NUM, 10);
        assert_eq!(MIN_ASYNC_SEND_SIZE, 1024 * 1024);
        assert_eq!(back_pressure_permits(1, MIN_ASYNC_SEND_NUM, "num"), 10);
        assert_eq!(back_pressure_permits(10, MIN_ASYNC_SEND_NUM, "num"), 10);
        assert_eq!(back_pressure_permits(99, MIN_ASYNC_SEND_NUM, "num"), 99);
        assert_eq!(
            back_pressure_permits(500, MIN_ASYNC_SEND_SIZE, "size"),
            MIN_ASYNC_SEND_SIZE
        );
    }
}
