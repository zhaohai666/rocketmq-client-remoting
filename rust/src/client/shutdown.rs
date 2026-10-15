//! Finalize-wait foundation for the synchronous `shutdown()`.
//!
//! The `shutdown()` of the push consumer / producer / pull consumer / lite pull consumer / trace dispatcher
//! are all **synchronous** APIs, but their finalization (in-flight redeliveries, last trace batch, code-35 unregister, final offset commit)
//! must run on the tokio runtime. If we only `handle.spawn(finalize)` and return, then when the caller
//! "returns right after shutdown() / calls `process::exit`" the finalization is cancelled as the runtime is dropped:
//! the redelivery never goes out => the message never reaches `%DLQ%`; the last trace batch never goes out => `RMQ_SYS_TRACE_TOPIC`
//! holds no records; the unregister never goes out => the broker waits ~120s of channel scanning before recycling this group.
//!
//! [`run_finalize_blocking`] therefore combines "detach the finalization" and "wait bounded for it to land" into
//! one step, branching on the calling context into three cases:
//!
//! 1. The target runtime is the **multi_thread** flavor:
//!    - the calling thread is on a worker of that multi-thread runtime: `block_in_place` hands over the worker
//!      core (the backup worker covers it, other tasks keep running), and waits in place;
//!    - the calling thread is not inside any runtime (plain std thread / the calling thread of a multi-thread `block_on`):
//!      block and wait directly.
//! 2. The target runtime is the **current_thread** flavor: its tasks only progress on the thread that drives it,
//!    and blocking the calling thread may be exactly the thread being pinned -- so all we can do is detach and return immediately, warning
//!    that "if the process exits now, finalization is lost".
//! 3. The calling thread itself is in a **current_thread** context (the target may be multi-thread): there
//!    `block_in_place` panics (tokio 1.53 `multi_thread/worker.rs:413-436`, the
//!    `allow_block_in_place=false` branch), and blocking in place may lock up the only driving thread
//!    -- so likewise detach, return immediately and warn.
//!
//! The wait cap is the caller-supplied `budget` (each call site uses [`SHUTDOWN_FINALIZE_BUDGET`]);
//! each finalizer has finer per-step caps inside (push drain 10s, traces 5s, unregister 3s etc.), so the budget
//! is only the outer fuse. Wait timeouts and dropped tasks are named in the log, never silent.

use std::future::Future;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};

use crate::rmq_warn;

/// Total budget for the inlined wait during `shutdown()` finalization (the outer fuse).
///
/// Each call site already has finer per-step limits inside its finalization (push drain
/// 10s ×2, trace 5s, unregister 3s, etc.); this is only a fallback for abnormal cases, so the caller is never held up indefinitely.
pub(crate) const SHUTDOWN_FINALIZE_BUDGET: Duration = Duration::from_secs(30);

/// Spawns `finalize` on the runtime behind `handle` and waits for it, with a bound, on
/// the calling thread until it lands.
///
/// Argument `what` is a log prefix (e.g. `"push consumer shutdown"`). Returns `true` if
/// finalization ran to completion within the budget; `false` means the wait was given up
/// (it may still be running in the background, or be cancelled when the runtime drops -- see cases 2/3 in the module header).
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
        // A failed send only means the caller gave up waiting (timeout / early return); it says nothing about the finalization outcome.
        let _ = tx.send(());
    });

    // The dispatch decision must be made at the call site (tokio 1.53 block_in_place
    // semantics): on a multi-thread worker it yields the core; on the calling thread of a
    // multi-thread `block_on` it passes through unchanged; in a current_thread context it panics; outside any runtime it is purely blocking.
    let outcome = match Handle::try_current() {
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| rx.recv_timeout(budget))
        }
        Ok(_) => {
            // current-thread context: block_in_place would panic, and blocking in place
            // would pin the only driver thread. Finalization is already detached; it finishes as long as the runtime stays alive.
            rmq_warn!(
                "{what}: called from a current-thread runtime; not waiting for \
                 finalization (task spawned on the target runtime)"
            );
            return false;
        }
        // Not inside any runtime (plain std thread): wait directly. The target runtime's
        // own workers drive the finalization and are unaffected by blocking here.
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
            // The finalization task was dropped before its send: the runtime is already
            // torn down (task cancelled), or the finalization future panicked.
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

    /// On a multi-thread runtime: returns `true` only after finalization completes, and "returned ⇒ done".
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
        assert!(done.load(Ordering::Acquire), "must have completed by the time we return");
    }

    /// Over budget: returns `false` (with a warning), but finalization is not cut off --
    /// it keeps running while the runtime is alive.
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
        assert!(!done.load(Ordering::Acquire), "should not have completed within the budget");
        // Finalization was not cancelled: wait one beat and it can be seen wrapping up.
        for _ in 0..100 {
            if done.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(done.load(Ordering::Acquire), "timeout only gives up the wait, it does not cancel");
    }

    /// Calling thread is not inside any runtime (plain std thread): blocks directly until done.
    #[test]
    fn waits_from_plain_thread() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
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

    /// Target runtime is current_thread: returns `false` right after detaching (cannot
    /// block that single driver thread).
    #[tokio::test]
    async fn refuses_to_block_on_current_thread_runtime() {
        let handle = Handle::current();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {
            flag.store(true, Ordering::Release);
        });
        assert!(!ok);
        // Finalization is a detached task: one yield should be enough for it to run (this test's runtime is still alive).
        for _ in 0..100 {
            if ran.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ran.load(Ordering::Acquire));
    }

    /// Target runtime is already torn down: the spawned finalization is cancelled ⇒
    /// `Disconnected` ⇒ `false` (no panic, no waiting out the timeout).
    #[test]
    fn reports_dropped_task_when_runtime_is_gone() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let handle = runtime.handle().clone();
        drop(runtime);
        let ok = run_finalize_blocking(&handle, "test", Duration::from_secs(5), async move {});
        assert!(!ok);
    }
}
