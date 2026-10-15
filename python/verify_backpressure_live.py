# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.backpressure import MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from remoting.exception import RemotingTooMuchRequestException

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
GROUP = "PID_rmq_bp_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


class _Recorder:
    """Thread-safe callback recorder (a duck type of SendCallback)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.results = []
        self.errors = []

    def on_success(self, send_result):
        with self.lock:
            self.results.append(send_result)

    def on_exception(self, e):
        with self.lock:
            self.errors.append(e)

    @property
    def done(self):
        with self.lock:
            return len(self.results) + len(self.errors)

    def summary(self):
        with self.lock:
            first = str(self.errors[0]) if self.errors else ""
            return "ok=%d err=%d %s" % (len(self.results), len(self.errors), first)


class _SlowHook:
    """Sleep for a while in the before hook of ``sendKernelImpl``.

    Permits are taken at the gate and returned at the end of the chain, so the cleanest way to hold in-flight slots is to slow the chain itself:
    blocking the user callback cannot hold a permit (it is returned one step before the result is handed to the user).
    """

    def __init__(self, millis=300):
        self.millis = millis

    def hook_name(self):
        return "slow-before"

    def send_message_before(self, context):
        time.sleep(self.millis / 1000.0)

    def send_message_after(self, context):
        pass


def _producer(instance_name, enable=True, num=None, size=None) -> DefaultMQProducer:
    p = DefaultMQProducer(GROUP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    p.set_enable_backpressure_for_async_mode(enable)
    if num is not None:
        p.set_back_pressure_for_async_send_num(num)
    if size is not None:
        p.set_back_pressure_for_async_send_size(size)
    return p


def _wait_until(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _start_all(threads):
    for t in threads:
        t.start()


def _join_all(threads, timeout=30.0):
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    stuck = sum(1 for t in threads if t.is_alive())
    check("*  send threads did not get stuck", stuck == 0, "%d still alive" % stuck)


def _landed_count(admin, topic, quiet=False):
    """How many messages landed on this topic on the broker (sum of maxOffset-minOffset across queues).

    This is the only hard evidence that "a rejected send never even sent its request": judging only by the client callback,
    an implementation that "errors the callback but actually sent the request" would also pass. Returns -1 when the route cannot be read.
    """
    try:
        queues = admin.examine_topic_route(topic).get_all_message_queue(topic)
    except Exception as e:  # noqa: BLE001 — route not registered yet
        if not quiet:
            print("    (failed to read %s route: %s)" % (topic, e))
        return -1
    total = 0
    for mq in queues:
        try:
            total += admin.max_offset(mq) - admin.min_offset(mq)
        except Exception:  # noqa: BLE001 — this queue was just created and has not been written yet
            pass
    return total


def _wait_landed(admin, topic, expected, timeout=30.0):
    """Wait until at least ``expected`` messages appear on the broker, returning the last reading.

    A new topic must wait for the broker to register the topicConfig incrementally to namesrv (seconds to tens of seconds), so
    the first few failed route reads are not failures; only retrying until timeout counts as failure.
    """
    deadline = time.time() + timeout
    landed = _landed_count(admin, topic, quiet=True)
    while landed < expected and time.time() < deadline:
        time.sleep(0.5)
        landed = _landed_count(admin, topic, quiet=True)
    if landed < 0:
        _landed_count(admin, topic)      # when the route truly cannot be read, print the reason to help locate the issue
    return landed


def _num_permits(p):
    return p.get_semaphore_async_send_num_available_permits()


def _size_permits(p):
    return p.get_semaphore_async_send_size_available_permits()


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    topics = []
    try:
        # ------------------------------------------------ B1 default capacity does not leak permits
        t1 = "BpDefault_%d" % STAMP
        topics.append(t1)
        p1 = _producer("bp-b1-%d" % STAMP)
        p1.start()
        rec = _Recorder()
        b1_threads = [threading.Thread(
            target=lambda i=i: p1.send_async(
                Message(t1, ("b1-%d" % i).encode() * 40), rec, 5000))
            for i in range(40)]
        _start_all(b1_threads)
        _join_all(b1_threads)
        check("B1 all 40 async sends completed their callbacks", _wait_until(lambda: rec.done >= 40, timeout=15.0),
              rec.summary())
        check("B1 all SEND_OK",
              all(r.send_status == SendStatus.SEND_OK for r in rec.results)
              and len(rec.results) == 40, rec.summary())
        landed = _wait_landed(admin, t1, 40)
        check("B1 exactly 40 landed on the broker", landed == 40, "landed=%d" % landed)
        check("B1 count permits fully returned", _num_permits(p1) == 1024,
              "available=%d" % _num_permits(p1))
        check("B1 byte permits fully returned", _size_permits(p1) == 100 * 1024 * 1024,
              "available=%d" % _size_permits(p1))
        p1.shutdown()

        # ------------------------------------- B2/B3 count gate: reject, reconcile, expand
        t2 = "BpNumGate_%d" % STAMP
        topics.append(t2)
        p2 = _producer("bp-b2-%d" % STAMP, num=MIN_ASYNC_SEND_NUM)
        slow2 = _SlowHook(600)
        p2.register_send_message_hook(slow2)
        p2.start()
        held, rejected, woken = _Recorder(), _Recorder(), _Recorder()
        holders = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"held"), held, 8000)) for _ in range(MIN_ASYNC_SEND_NUM)]
        _start_all(holders)
        check("B2 the free count drops to 0 once in-flight is full",
              _wait_until(lambda: _num_permits(p2) == 0, timeout=3.0),
              "available=%d" % _num_permits(p2))
        # The 11th and 12th sends: the budget is only 150ms, so they cannot wait for a permit
        rejected_threads = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"rejected"), rejected, 150)) for _ in range(2)]
        began = time.monotonic()
        _start_all(rejected_threads)
        _join_all(rejected_threads, timeout=10.0)
        check("B2 the gate only errors after the budget is exhausted (it does not reject at a glance)",
              int((time.monotonic() - began) * 1000) >= 130,
              "the caller waited %dms" % int((time.monotonic() - began) * 1000))
        check("B2 the two over-limit sends callback with TooMuchRequest, message matching the protocol contract verbatim",
              len(rejected.errors) == 2
              and all(isinstance(e, RemotingTooMuchRequestException) for e in rejected.errors)
              and all("send message tryAcquire semaphoreAsyncNum timeout" in str(e)
                      for e in rejected.errors),
              rejected.summary())
        _join_all(holders, timeout=25.0)
        check("B2 all 10 in-flight sends went out",
              _wait_until(lambda: len(held.results) == MIN_ASYNC_SEND_NUM, timeout=20.0),
              held.summary())
        landed2 = _wait_landed(admin, t2, MIN_ASYNC_SEND_NUM)
        check("B2 the two rejected sends left nothing on the broker (not even a request)",
              landed2 == MIN_ASYNC_SEND_NUM,
              "landed=%d expected %d" % (landed2, MIN_ASYNC_SEND_NUM))
        check("B2 count permits return to 10 after everything lands",
              _wait_until(lambda: _num_permits(p2) == MIN_ASYNC_SEND_NUM, timeout=20.0),
              "available=%d" % _num_permits(p2))

        # B3: fill 10 in-flight again and raise the capacity to 12 -- those stuck at the gate should be woken up.
        # This round sleeps the hook for 2s: the in-flight hold time must be far greater than the cost of "start thread + poll to confirm + start waiter",
        # otherwise permits are already returned before the check completes a full pass (at 300ms this was observed to miss).
        slow2.millis = 2000
        holders2 = _Recorder()
        round2 = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"held2"), holders2, 8000)) for _ in range(MIN_ASYNC_SEND_NUM)]
        _start_all(round2)
        check("B3 the second round fills in-flight just the same", _wait_until(lambda: _num_permits(p2) == 0, timeout=3.0),
              "available=%d" % _num_permits(p2))
        waiter = threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"woken"), woken, 8000))
        waiter.start()
        time.sleep(0.3)
        check("B3 before the expansion the caller really is stuck at the gate", waiter.is_alive() and _num_permits(p2) == 0,
              "alive=%s available=%d" % (waiter.is_alive(), _num_permits(p2)))
        p2.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 2)
        waiter.join(15.0)
        # The caller thread being woken up counts as "through the gate", but the result only reaches the callback after the whole chain finishes (this round the hook sleeps 2s),
        # so what we wait for here is the callback, not thread exit.
        check("B3 the expansion woke up the sender stuck at the gate and sent it",
              not waiter.is_alive() and _wait_until(
                  lambda: any(r.send_status == SendStatus.SEND_OK for r in woken.results),
                  timeout=20.0),
              woken.summary())
        _join_all(round2, timeout=25.0)
        check("B3 free permits = the new capacity 12 after everything is returned",
              _wait_until(lambda: _num_permits(p2) == MIN_ASYNC_SEND_NUM + 2, timeout=25.0),
              "available=%d" % _num_permits(p2))
        expect3 = 2 * MIN_ASYNC_SEND_NUM + 1
        landed3 = _wait_landed(admin, t2, expect3)
        check("B3 broker total = two rounds of in-flight + the woken send", landed3 == expect3,
              "landed=%d expected %d" % (landed3, expect3))
        p2.shutdown()

        # ------------------------------------------- B4 byte gate (1M floor + 600KB)
        t3 = "BpSizeGate_%d" % STAMP
        topics.append(t3)
        p3 = _producer("bp-b4-%d" % STAMP, num=MIN_ASYNC_SEND_NUM,
                       size=MIN_ASYNC_SEND_SIZE)
        p3.register_send_message_hook(_SlowHook(400))
        p3.start()
        big = b"4" * (600 * 1024)
        one, two = _Recorder(), _Recorder()
        th_big = threading.Thread(target=lambda: p3.send_async(Message(t3, big), one, 8000))
        th_big.start()
        check("B4 the in-flight byte permit is deducted by exactly the body length",
              _wait_until(lambda: _size_permits(p3) == MIN_ASYNC_SEND_SIZE - len(big),
                          timeout=3.0),
              "available=%d expected %d" % (_size_permits(p3), MIN_ASYNC_SEND_SIZE - len(big)))
        th_two = threading.Thread(target=lambda: p3.send_async(Message(t3, big), two, 150))
        th_two.start()
        th_two.join(10.0)
        check("B4 the second 600KB send cannot pass the byte gate, message matching the protocol contract verbatim",
              len(two.errors) == 1
              and "send message tryAcquire semaphoreAsyncSize timeout" in str(two.errors[0]),
              two.summary())
        check("B4 the count permit is already returned when the byte gate fails",
              _wait_until(lambda: _num_permits(p3) == MIN_ASYNC_SEND_NUM, timeout=3.0),
              "available=%d" % _num_permits(p3))
        th_big.join(25.0)
        check("B4 the first send landed normally", _wait_until(lambda: one.done >= 1, timeout=20.0),
              one.summary())
        landed4 = _wait_landed(admin, t3, 1)
        check("B4 only the first send is on the broker (the rejected one left no trace)", landed4 == 1, "landed=%d" % landed4)
        check("B4 everything returned: both count and bytes are back to the configured amount",
              _num_permits(p3) == MIN_ASYNC_SEND_NUM
              and _size_permits(p3) == MIN_ASYNC_SEND_SIZE,
              "num=%d size=%d" % (_num_permits(p3), _size_permits(p3)))
        p3.shutdown()

        # ------------------------------------------------ B5 backpressure off means no throttling
        t4 = "BpOff_%d" % STAMP
        topics.append(t4)
        p4 = _producer("bp-b5-%d" % STAMP, enable=False, num=MIN_ASYNC_SEND_NUM,
                       size=MIN_ASYNC_SEND_SIZE)
        p4.register_send_message_hook(_SlowHook(200))
        p4.start()
        rec5 = _Recorder()

        def _body(i):
            # One in three is 300KB: with backpressure off the byte gate is not even consulted, but with it on this would certainly throttle
            return Message(t4, b"x" * (300 * 1024) if i % 3 == 0 else b"small")

        b5_threads = [threading.Thread(
            target=lambda i=i: p4.send_async(_body(i), rec5, 8000))
            for i in range(30)]
        _start_all(b5_threads)
        _join_all(b5_threads)
        check("B5 with backpressure off, all 30 concurrent sends succeed", _wait_until(lambda: rec5.done >= 30, timeout=25.0),
              rec5.summary())
        check("B5 all SEND_OK", len(rec5.results) == 30, rec5.summary())
        landed5 = _wait_landed(admin, t4, 30)
        check("B5 all 30 are on the broker", landed5 == 30, "landed=%d" % landed5)
        p4.shutdown()
    finally:
        addr = _broker_addr(admin)
        for topic in topics:
            try:
                admin.delete_topic_in_broker(addr, topic)
            except Exception:  # noqa: BLE001 — cleanup failure does not affect the conclusion
                pass
        admin.shutdown()

    failed = [name for name, ok, _ in results if not ok]
    print("\n%d PASS / %d FAIL" % (len(results) - len(failed), len(failed)))
    for name in failed:
        print("  FAILED: %s" % name)
    return 1 if failed else 0


def _broker_addr(admin):
    try:
        return admin.fetch_broker_cluster_info().get_broker_addrs()[0]
    except Exception:  # noqa: BLE001
        return "127.0.0.1:10911"


if __name__ == "__main__":
    sys.exit(main())
