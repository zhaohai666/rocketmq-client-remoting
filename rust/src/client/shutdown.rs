//! 同步 `shutdown()` 的收尾等待基座。
//!
//! push 消费者 / 生产者 / 拉消费者 / lite 拉消费者 / 轨迹分发器的 `shutdown()`
//! 都是**同步** API，但它们的收尾（在途回投、末批轨迹、35 号注销、末次位点提交）
//! 必须挂在 tokio 运行时上跑。若只 `handle.spawn(收尾)` 就返回，调用方
//! 「shutdown() 后立刻 return / `process::exit`」时收尾会随运行时析构被取消：
//! 回投没发出去 ⇒ 消息进不了 `%DLQ%`；末批轨迹没发出去 ⇒ `RMQ_SYS_TRACE_TOPIC`
//! 里没有记录；35 号注销没发出去 ⇒ broker 要等 ~120s 通道扫描才回收本组。
//!
//! [`run_finalize_blocking`] 因此把「挂出收尾」与「有界等待落地」合成一步，按
//! 调用现场分三种情形：
//!
//! 1. 目标运行时是**多线程** flavor：
//!    - 调用线程在当前多线程运行时的 worker 上：`block_in_place` 交出 worker
//!      core（由兜底 worker 顶上，其余任务照跑），原地等；
//!    - 调用线程不在任何运行时里（纯 std 线程 / 多线程 `block_on` 的调用线程）：
//!      直接阻塞等待。
//! 2. 目标运行时是 **current_thread** flavor：它的任务只在驱动它的那个线程上
//!    推进，阻塞调用线程可能正把那个线程钉死 —— 只能挂出后立即返回，并告警
//!    说明「进程若现在退出，收尾会丢」。
//! 3. 调用线程本身在 **current_thread** 上下文里（目标可能是多线程）：此时
//!    `block_in_place` 会 panic（tokio 1.53 `multi_thread/worker.rs:413-436` 的
//!    `allow_block_in_place=false` 分支），就地阻塞又可能把唯一驱动线程锁死
//!    —— 同样挂出后立即返回并告警。
//!
//! 等待上限是调用方给的 `budget`（各调用点用 [`SHUTDOWN_FINALIZE_BUDGET`]）；
//! 各收尾内部还有更细的分步上限（push 排空 10s、轨迹 5s、注销 3s 等），预算
//! 只是外层保险丝。等待超时、任务被取消都在日志里点名，不静默。

use std::future::Future;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};

use crate::rmq_warn;

/// `shutdown()` 内联等待收尾的总预算（外层保险丝）。
///
/// 各调用点的收尾内部已有更细的分步上限（push 排空 10s ×2、轨迹 5s、注销 3s
/// 等），这里只兜底异常情况，避免把调用方无限期拖住。
pub(crate) const SHUTDOWN_FINALIZE_BUDGET: Duration = Duration::from_secs(30);

/// 把 `finalize` 挂到 `handle` 对应的运行时上执行，并在调用线程上有界等待
/// 它落地。
///
/// 参数 `what` 是日志前缀（如 `"push consumer shutdown"`）。返回 `true` 表示
/// 收尾在预算内完整跑完；`false` 表示已放弃等待（收尾可能仍在后台继续，也可能
/// 随运行时析构被取消 —— 见模块头的情形 2/3）。
pub(crate) fn run_finalize_blocking<F>(
    handle: &Handle,
    what: &str,
    budget: Duration,
    finalize: F,
) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        handle.spawn(finalize);
        rmq_warn!(
            "{what}: current-thread runtime cannot be blocked; finalization was \
             detached and is lost if the process exits now"
        );
        return false;
    }

    let (tx, rx) = std::sync::mpsc::channel::<()>();
    handle.spawn(async move {
        finalize.await;
        // 发送失败只说明调用方已放弃等待（超时 / 提前返回），与收尾结果无关。
        let _ = tx.send(());
    });

    // 判定必须在调用现场做（tokio 1.53 的 block_in_place 语义）：
    // 多线程 worker 上会让出 core；多线程 `block_on` 的调用线程上原样放行；
    // current_thread 上下文里会 panic；不在任何运行时里则纯阻塞。
    let outcome = match Handle::try_current() {
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| rx.recv_timeout(budget))
        }
        Ok(_) => {
            // current_thread 上下文：block_in_place 会 panic，就地阻塞又会把
            // 唯一驱动线程钉死。收尾已挂出，运行时活着就会跑完。
            rmq_warn!(
                "{what}: called from a current-thread runtime; not waiting for \
                 finalization (task spawned on the target runtime)"
            );
            return false;
        }
        // 不在任何运行时里（纯 std 线程）：直接等。目标运行时的 worker 自己
        // 在推进收尾，不受这里阻塞影响。
        Err(_) => rx.recv_timeout(budget),
    };

    match outcome {
        Ok(()) => true,
        Err(RecvTimeoutError::Timeout) => {
            rmq_warn!(
                "{what}: finalization still running after {budget:?}, \
                 returning without further waiting"
            );
            false
        }
        Err(RecvTimeoutError::Disconnected) => {
            // 收尾任务在 send 之前被丢弃：运行时已拆（任务被取消），
            // 或收尾 future panic 了。
            rmq_warn!(
                "{what}: finalization task dropped before completion \
                 (runtime already shut down, or it panicked)"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;

    /// 多线程运行时里：收尾跑完才返回 `true`，且「已返回 ⇒ 已落地」。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waits_for_finalization_on_multi_thread_runtime() {
        let handle = Handle::current();
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag.store(true, Ordering::Release);
        });
        assert!(ok);
        assert!(done.load(Ordering::Acquire), "返回时必须已经跑完");
    }

    /// 超出预算：返回 `false`（告警），但收尾不会被掐掉 —— 运行时活着时它继续
    /// 跑完。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn times_out_but_leaves_finalization_running() {
        let handle = Handle::current();
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let start = Instant::now();
        let ok = run_finalize_blocking(&handle, "test", Duration::from_millis(50), async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            flag.store(true, Ordering::Release);
        });
        assert!(!ok);
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert!(!done.load(Ordering::Acquire), "预算内不该已经跑完");
        // 收尾没被取消：等一拍就能看到它收口。
        for _ in 0..100 {
            if done.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(done.load(Ordering::Acquire), "超时只是放弃等待，不是取消");
    }

    /// 调用线程不在任何运行时里（纯 std 线程）：直接阻塞等完。
    #[test]
    fn waits_from_plain_thread() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("多线程运行时");
        let handle = runtime.handle().clone();
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {
            flag.store(true, Ordering::Release);
        });
        assert!(ok);
        assert!(done.load(Ordering::Acquire));
        runtime.shutdown_background();
    }

    /// 目标运行时是 current_thread：挂出后立即返回 `false`（不能阻塞那个唯一
    /// 驱动线程）。
    #[tokio::test]
    async fn refuses_to_block_on_current_thread_runtime() {
        let handle = Handle::current();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {
            flag.store(true, Ordering::Release);
        });
        assert!(!ok);
        // 收尾是挂出的游离任务：让出一次就该跑（本测试的运行时还活着）。
        for _ in 0..100 {
            if ran.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ran.load(Ordering::Acquire));
    }

    /// 目标运行时已经被拆：`spawn` 出去的收尾被取消 ⇒ `Disconnected` ⇒ `false`
    /// （不 panic、不等到超时）。
    #[test]
    fn reports_dropped_task_when_runtime_is_gone() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("多线程运行时");
        let handle = runtime.handle().clone();
        drop(runtime);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {});
        assert!(!ok);
    }
}
