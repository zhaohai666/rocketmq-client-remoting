//! * `semaphoreAsyncSendNum` —— 在途**条数**，`back_pressure_for_async_send_num` 默认 1024，
//!   地板 10；
//! * `semaphoreAsyncSendSize` —— 在途**字节数**，`back_pressure_for_async_send_size` 默认 100M，
//!   地板 1M，一笔消息扣掉 `body.len()` 个许可（body 为空按 1 算）。
//!
//! 两个许可都用**整个剩余预算**去等，等不到就直接回调
//! `Error::TooMuchRequest("send message tryAcquire semaphoreAsyncNum timeout")`
//! （第二个是 `...semaphoreAsyncSize timeout`），一次请求都不会发出去。
//!
//! # 与 Python/C++/C# 端口的两处结构差别

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::rmq_info;

pub const MIN_ASYNC_SEND_NUM: i64 = 10;
pub const MIN_ASYNC_SEND_SIZE: i64 = 1024 * 1024;

/// 一个还在排队的许可申请（只为让队首能被稳定识别，不代表已拿到许可）。
///
/// 这里不存 `permits`：等待者每次醒来都带着自己那份申请量来看队列（`grant_if_head`
/// 的入参），票据只需要能被认出来。
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

/// * 不需要那层自旋读写锁 —— 改容量与拿/还许可在对象内部天然互斥；
/// * 改容量不会把等待者丢在旧对象上，改完它们会带着新容量继续等。
pub struct FairSemaphore {
    state: Mutex<State>,
    /// 对应 Python 的 `threading.Condition`：这里「广播」= `notify_waiters`，
    /// 每个等待者被叫醒后自己重看队列（只有队首能过）。
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

    /// ⚠ 拿到许可和**放弃排队**这两个出口都必须再叫醒一次：公平模式下只有队首能拿，
    /// 队首一换人，后面的申请就可能从「轮不到我」变成「该我了」，而它的 `permits` 数量
    /// 未必被前一个人的动作影响（队首要 5 个、空闲 6 个时，队首拿走 5 个后剩下 1 个，
    /// 正好够排在第二的那 1 个 —— 但 `release` 早就跑完了，没人为它叫醒）。
    /// 少叫醒这一次，那个人就会一直睡到自己的超时：真机上是 5 秒死等，不是丢一条消息。
    pub async fn try_acquire(&self, permits: i64, timeout_millis: i64) -> bool {
        let budget = u64::try_from(timeout_millis.max(0)).unwrap_or(u64::MAX);
        let deadline = Instant::now() + Duration::from_millis(budget);
        let _ticket = Ticket::new(self);
        loop {
            // 先登记唤醒凭据、再看队列：`notify_waiters` 只叫醒当时已在等的人
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.grant_if_head(_ticket.id, permits) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // 预算用尽：票据 Drop 时把自己从队列里摘掉，别挡后面的人
                return false;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return false;
            }
            // 被叫醒：回到循环顶重看队列（也可能是假醒，重看自然再睡回去）
        }
    }

    /// 队首且空闲许可够，才算拿到；拿到时顺手叫醒后面的人。
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

    /// 当前空闲许可（可能是负数，见 [`set_total_permits`](Self::set_total_permits)）。
    pub fn available_permits(&self) -> i64 {
        lock(&self.state).free
    }

    /// 当前总容量。
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

/// 排队票据：`Drop` 负责在等待方**没有**拿到许可时把自己摘出去并叫醒后面的人。
///
/// 拿到许可时票据已被 `grant_if_head` 弹出，Drop 什么都找不到 —— 不需要额外的
///「已成交」标记，也因此不存在「忘记 disarm 就把队首空掉」这类 bug。
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
            // 挡路的人走了：队首换人，下一个人可能就够了
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

    /// 轮询等待条件成立（最多约 1s）。
    ///
    /// 只用 `yield_now` 不行：单线程运行时里它会让出调度但不放时间，几个任务互相
    /// 等对方先把票据排进队列时就会转成死循环（本模块第一版就死在这里）。
    async fn wait_until<F: Fn() -> bool>(cond: F) -> bool {
        for _ in 0..500 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        cond()
    }

    /// 等到队列里恰好有 `n` 张票 —— 公平性用例要靠它把「到达顺序」钉住。
    async fn wait_queue_len(sem: &FairSemaphore, n: usize) {
        assert!(
            wait_until(|| sem.waiting_count() == n).await,
            "第 {n} 个申请人没能排上队（当前 {}）",
            sem.waiting_count()
        );
    }

    /// 在后台跑一次 `try_acquire`，返回是否拿到、等了多久（真实时钟）。
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
        // 等 late 真的排上了，否则下面的判定只是运气
        wait_queue_len(&sem, 1).await;
        // 队首要 1 个、空闲 0 个：插不上队
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
            // 一个一个钉住到达顺序：第 i 个人排上队之后才放第 i+1 个人
            wait_queue_len(&sem, i + 1).await;
        }
        sem.release(3);
        for h in handles {
            assert!(h.await.unwrap());
        }
        // 公平 = 严格按到达顺序放行，不是「谁先被叫醒谁先拿」
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

    /// Python `test_a_granted_head_still_feeds_the_waiter_behind_it`。
    ///
    /// 空闲 6、队首要 5、第二个人要 1：两个人都该拿到，且都不该等到超时。
    /// 锁的是「队首离场时也必须叫醒后面的人」——`release` 早就跑完了，没人再喊第二
    /// 个人的话，他明明够用却能睡满 3000ms。
    #[tokio::test]
    async fn a_granted_head_still_feeds_the_waiter_behind_it() {
        let sem = semaphore(6);
        assert!(sem.try_acquire(5, 0).await); // 在途 5，空闲 1
        let head = acquire_in_background(&sem, 5, 3000);
        wait_queue_len(&sem, 1).await;
        // 空闲 1 个就够第二个人 —— 但公平模式下他必须等队首先走
        let second = acquire_in_background(&sem, 1, 3000);
        wait_queue_len(&sem, 2).await;
        sem.release(5); // 空闲 1 → 6：队首这就够了

        let (head_got, _) = head.await.unwrap();
        let (second_got, second_waited) = second.await.unwrap();
        assert!(head_got);
        assert!(second_got, "队首拿走许可后，第二个人被丢下了（丢唤醒）");
        assert!(
            second_waited < Duration::from_millis(2000),
            "second waited {second_waited:?}"
        );
        assert_eq!(sem.available_permits(), 0); // 6 - 5 - 1
    }

    /// Python `test_a_timed_out_head_wakes_the_waiter_behind_it`。
    ///
    /// 丢唤醒的回归守卫：队首要的比总容量还多，永远拿不到，只能等满自己的预算走人 ——
    /// 他离场时必须叫醒后面的人，否则第二个人明明能过却要睡满 3000ms。真机上的表现是
    /// 异步发送白等满预算，再回调一个 `semaphoreAsyncNum timeout`，而许可早就空了。
    #[tokio::test]
    async fn a_timed_out_head_wakes_the_waiter_behind_it() {
        let sem = semaphore(3);
        assert!(sem.try_acquire(3, 0).await); // 掏空
        let head = acquire_in_background(&sem, 4, 200); // 要的比总量还多
        wait_queue_len(&sem, 1).await;
        let second = acquire_in_background(&sem, 1, 3000);
        wait_queue_len(&sem, 2).await;
        sem.release(3); // 够 second，但队首是那个贪心的

        let (head_got, _) = head.await.unwrap();
        let (second_got, second_waited) = second.await.unwrap();
        assert!(!head_got, "要得比总量还多，本该拿不到");
        assert!(second_got, "队首退出后没被叫醒（丢唤醒）");
        assert!(
            second_waited < Duration::from_millis(2000),
            "second waited {second_waited:?}"
        );
    }

    #[tokio::test]
    async fn a_dropped_waiter_leaves_no_ghost_ticket() {
        // 取消安全：等待方半路被丢弃（任务 abort / select! 落败）不能把队首永久堵死。
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
        assert!(got, "幽灵票据把队首堵死了");
    }

    #[tokio::test]
    async fn an_expired_budget_grants_only_when_the_head_is_clear() {
        let sem = semaphore(2);
        // 预算 0 但队首无人、许可够：与 Python 一致，仍然给（先看队列再看时限）
        assert!(sem.try_acquire(1, 0).await);
        assert!(sem.try_acquire(1, 0).await);
        // 此时空闲 0：0 预算立刻失败
        assert!(!sem.try_acquire(1, 0).await);
        assert!(!sem.try_acquire(1, -5).await);
        assert_eq!(sem.waiting_count(), 0, "拿不到也不该留下票据");
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
