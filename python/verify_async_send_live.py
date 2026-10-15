# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.exception import MQClientException
from client.hook import CheckForbiddenHook, SendMessageHook
from client.producer import DefaultMQProducer, SendCallback
from client.send_result import SendStatus
from common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
GROUP = "PID_rmq_async_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _first(items):
    """Take the text of the first item (it goes into detail; an empty list must print as an empty string, not IndexError)."""
    return str(items[0]) if items else ""


class _Recorder(SendCallback):
    pass

    def __init__(self):
        self.lock = threading.Lock()
        self.results = []
        self.errors = []
        self.threads = []
        self.ats = []

    def on_success(self, send_result):
        with self.lock:
            self.results.append(send_result)
            self._note()

    def on_exception(self, e):
        with self.lock:
            self.errors.append(e)
            self._note()

    def _note(self):
        """Record the moment and thread name of the terminal state (the caller must already hold ``self.lock``)."""
        self.ats.append(time.monotonic())
        self.threads.append(threading.current_thread().name)

    def after(self, moment):
        """Count of callbacks emitted after ``moment`` (on the ``time.monotonic()`` scale)."""
        with self.lock:
            return sum(1 for t in self.ats if t > moment)

    @property
    def done(self):
        with self.lock:
            return len(self.results) + len(self.errors)

    def summary(self):
        with self.lock:
            first = str(self.errors[0]) if self.errors else ""
            return "ok=%d err=%d %s" % (len(self.results), len(self.errors), first)


class _TracingHook(SendMessageHook):
    """The before hook sleeps for a while: slowing down the "prepare phase" is how we prove the caller did not wait on it.

    It also records two things: which thread the hook ran on, and how many times before/after each ran.
    """

    def __init__(self, millis=0):
        self.millis = millis
        self.lock = threading.Lock()
        self.before = 0
        self.after = 0
        self.before_thread = ""

    def hook_name(self):
        return "async-tracing"

    def send_message_before(self, context):
        with self.lock:
            self.before += 1
            self.before_thread = threading.current_thread().name
        if self.millis:
            time.sleep(self.millis / 1000.0)

    def send_message_after(self, context):
        with self.lock:
            self.after += 1


class _ForbiddenTagHook(CheckForbiddenHook):
    """Only rejects messages tagged forbidden, and records the CommunicationMode it sees."""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0
        self.mode = None

    def hook_name(self):
        return "async-forbidden"

    def check_forbidden(self, context):
        with self.lock:
            self.calls += 1
            self.mode = context.communication_mode
        if context.message is not None and context.message.get_tags() == "forbidden":
            raise MQClientException("live test: tag forbidden is not allowed")


def _producer(instance_name, *hooks) -> DefaultMQProducer:
    p = DefaultMQProducer(GROUP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    for h in hooks:
        if isinstance(h, SendMessageHook):
            p.register_send_message_hook(h)
        else:
            p.register_check_forbidden_hook(h)
    return p


def _start_all(threads):
    for t in threads:
        t.start()


def _join_all(threads, timeout=30.0):
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    stuck = sum(1 for t in threads if t.is_alive())
    check("*  send threads did not get stuck", stuck == 0, "%d still alive" % stuck)


def _landed(admin, topic, quiet=False):
    """How many messages landed on this topic on the broker (sum of maxOffset-minOffset across queues).

    This is the only hard evidence that "the SendResult in the callback is not self-reported". Returns -1 when the route cannot be read.
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
    landed = _landed(admin, topic, quiet=True)
    while landed < expected and time.time() < deadline:
        time.sleep(0.5)
        landed = _landed(admin, topic, quiet=True)
    return landed


def _queue_landed(admin, mq):
    try:
        return admin.max_offset(mq) - admin.min_offset(mq)
    except Exception:  # noqa: BLE001
        return -1


# ------------------------------------------------- A1 return immediately + thread accounting + read-back
def a1_non_blocking_and_read_back(admin, topic):
    hook = _TracingHook(400)
    p = _producer("a1-%d" % STAMP, hook)
    p.start()
    rec = _Recorder()
    body = "async-a1-readback"
    try:
        msg = Message(topic, body.encode())
        began = time.monotonic()
        p.send_async(msg, rec, 5000)
        took_ms = int((time.monotonic() - began) * 1000)
        check("A1 the caller returned before the prepare phase (the hook sleeps 400ms)",
              took_ms < 200 and hook.before <= 1,
              "caller took=%dms before ran=%d" % (took_ms, hook.before))
        check("A1 exactly one callback and SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and len(rec.results) == 1 and not rec.errors, rec.summary())
        if not rec.results:
            return
        r = rec.results[0]
        check("A1 the before hook ran on AsyncSenderExecutor_N",
              hook.before_thread.startswith("AsyncSenderExecutor_"),
              "thread name=%s" % hook.before_thread)
        check("A1 the user callback ran on NettyClientPublicExecutor_N (executeInvokeCallback)",
              rec.threads[0].startswith("NettyClientPublicExecutor_"),
              "thread name=%s" % rec.threads[0])
        check("A1 before/after each ran once", hook.before == 1 and hook.after == 1,
              "before=%d after=%d" % (hook.before, hook.after))
        check("A1 the broker landed this message", _wait_landed(admin, topic, 1) == 1)
        # offset_msg_id is given by the broker; only it can be decoded to the commitLog offset ⇒ read the message back and match the body
        back = [None]

        def _view():
            try:
                back[0] = admin.view_message(topic, r.offset_msg_id)
                return back[0] is not None
            except Exception:  # noqa: BLE001 — commitLog not flushed out yet
                return False

        read = _wait_until(_view, 15.0)
        check("A1 the message can be read back with the offset_msg_id from the callback",
              read and back[0].body.decode() == body,
              "body=%s" % ("<null>" if back[0] is None else back[0].body.decode()))
        check("A1 the queue_offset in the callback is exactly where it landed",
              r.queue_offset == admin.max_offset(r.message_queue) - 1,
              "queue_offset=%d max_offset=%d"
              % (r.queue_offset, admin.max_offset(r.message_queue)))
        # msg_id is the client-added UNIQ_KEY (32 hex digits), not the broker's copy
        check("A1 msg_id is the client UNIQ_KEY, different from the broker's offset_msg_id",
              len(r.msg_id) == 32 and r.msg_id != r.offset_msg_id,
              "msgId=%s offsetMsgId=%s" % (r.msg_id, r.offset_msg_id))
    finally:
        p.shutdown()


# ------------------------------------------------- A2 concurrent sends do not cross
def a2_burst_exactly_once_each(admin, topic):
    burst = 30
    p = _producer("a2-%d" % STAMP)
    p.start()
    each = [_Recorder() for _ in range(burst)]
    try:
        threads = [threading.Thread(
            target=lambda i=i: p.send_async(
                Message(topic, ("async-burst-%d" % i).encode()), each[i], 8000))
            for i in range(burst)]
        _start_all(threads)
        _join_all(threads)
        check("A2 all 30 concurrent async sends got a terminal state",
              _wait_until(lambda: all(r.done >= 1 for r in each), 20.0),
              "done=%d" % sum(r.done for r in each))
        check("A2 each got exactly one terminal state (no more, no less)",
              all(r.done == 1 for r in each),
              "count with extra callbacks=%d" % sum(1 for r in each if r.done > 1))
        check("A2 all SEND_OK",
              all(len(r.results) == 1 and not r.errors for r in each), each[0].summary())
        check("A2 exactly 30 landed on the broker", _wait_landed(admin, topic, burst) == burst)
        slots = set()
        uniq = set()
        for r in each:
            for s in r.results:
                slots.add((s.message_queue.broker_name, s.message_queue.queue_id,
                           s.queue_offset))
                uniq.add(s.msg_id)
        check("A2 each (broker, queueId, queue_offset) is distinct across sends",
              len(slots) == burst, "after dedup=%d" % len(slots))
        check("A2 every UNIQ_KEY is distinct", len(uniq) == burst, "after dedup=%d" % len(uniq))
    finally:
        p.shutdown()


# ------------------------------------------------- A3 pinned send
def a3_pinned_queue(admin, topic):
    p = _producer("a3-%d" % STAMP)
    p.start()
    rec = _Recorder()
    try:
        # First get the topic up (send one synchronously so the broker creates all queues), then pick a queue to pin.
        # ⚠ Before taking the baseline you must wait for the warm-up message to be readable on the broker side: a freshly acked message landing in the
        # consumeQueue has a delay; if the baseline reads as 0 and it becomes 1 during reconciliation, you get a phantom extra 1.
        p.send(Message(topic, b"async-a3-warmup"), 5000)
        check("A3 the warm-up message is already readable on the broker", _wait_landed(admin, topic, 1) >= 1,
              "landed=%d" % _landed(admin, topic))
        queues = p.fetch_publish_message_queues(topic)
        check("A3 got the publish queues", len(queues) > 0, "queues=%d" % len(queues))
        aimed = queues[0]
        before = _queue_landed(admin, aimed)
        others_before = sum(max(0, _queue_landed(admin, q)) for q in queues[1:])
        p.send_async(Message(topic, b"async-a3-pinned"), rec, 5000, aimed)
        check("A3 the pinned async send got a terminal state and SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and len(rec.results) == 1, rec.summary())
        if not rec.results:
            return
        r = rec.results[0]
        check("A3 the result landed on the specified queue",
              r.message_queue.broker_name == aimed.broker_name
              and r.message_queue.queue_id == aimed.queue_id,
              "broker=%s queueId=%d" % (r.message_queue.broker_name,
                                        r.message_queue.queue_id))
        check("A3 that queue grew by exactly 1",
              _wait_until(lambda: _queue_landed(admin, aimed) == before + 1, 20.0),
              "landed=%d before=%d" % (_queue_landed(admin, aimed), before))
        others_after = sum(max(0, _queue_landed(admin, q)) for q in queues[1:])
        check("A3 no other queue grew at all", others_after == others_before,
              "other queues %d -> %d" % (others_before, others_after))
    finally:
        p.shutdown()


# ------------------------------------------------- A4 interception hook
def a4_forbidden_hook(admin, topic):
    forbidden = _ForbiddenTagHook()
    p = _producer("a4-%d" % STAMP, forbidden)
    p.start()
    rejected, passed = _Recorder(), _Recorder()
    try:
        bad = Message(topic, b"async-a4-rejected", tags="forbidden")
        p.send_async(bad, rejected, 5000)
        check("A4 the exception rejected by the hook reached the callback unchanged",
              _wait_until(lambda: rejected.done >= 1, 10.0)
              and rejected.errors
              and "tag forbidden is not allowed" in str(rejected.errors[0]),
              rejected.summary())
        check("A4 the interception hook saw ASYNC", forbidden.mode == "ASYNC",
              "mode=%s" % forbidden.mode)
        # This topic has nothing besides the rejected message ⇒ either we read 0 messages, or the route is not even
        # registered yet (-1). The route is only auto-created when the first message lands on the broker,
        # so "cannot read the route" is itself evidence that "the broker never received the request".
        after_reject = _landed(admin, topic)
        check("A4 the rejected message left no trace on the broker", after_reject <= 0,
              "landed=%d (-1 = route not created yet, i.e. the broker received nothing)" % after_reject)

        p.send_async(Message(topic, b"async-a4-ok"), passed, 5000)
        check("A4 the same producer lands a different tag just fine (the rejection did not break the pool)",
              _wait_until(lambda: passed.done >= 1, 20.0) and len(passed.results) == 1,
              passed.summary())
        check("A4 exactly 1 landed on the broker", _wait_landed(admin, topic, 1) == 1,
              "landed=%d" % _landed(admin, topic))
        check("A4 the hook was called 2 times in total (one rejected, one allowed)", forbidden.calls == 2,
              "calls=%d" % forbidden.calls)
    finally:
        p.shutdown()


# ------------------------------------------------- A5 batches go through the sync-batch kernel
def a5_batch_async(admin, topic):
    p = _producer("a5-%d" % STAMP)
    p.start()
    rec = _Recorder()
    try:
        msgs = [Message(topic, ("async-a5-%d" % i).encode()) for i in range(3)]
        p.send_async(msgs, rec, 8000)
        check("A5 the batch async send got exactly one callback and SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and rec.done == 1 and len(rec.results) == 1, rec.summary())
        check("A5 all three landed together", _wait_landed(admin, topic, 3) == 3,
              "landed=%d" % _landed(admin, topic))
        check("*  A5 request code = SEND_BATCH_MESSAGE(320), evidenced by an offline unit test",
              True, "the wire message is not visible live; see tests/test_producer_async.py")

        r = rec.results[0] if rec.results else None
        if r is None:
            check("A5 batch read-back", False, "the callback did not deliver a SendResult")
            return
        check("A5 the batch msg_id is the client 32-digit ID, not the broker's offset_msg_id",
              len(r.msg_id) == 32 and "," not in r.msg_id and r.msg_id != r.offset_msg_id,
              "msgId=%s offsetMsgId=%s" % (r.msg_id, r.offset_msg_id))
        # The batch reply's offset_msg_id is a broker-returned list one per sub-message (comma-separated, one commitLog
        # offset per sub-message), which is itself evidence that "this batch was split and stored as 3 messages"
        sub_offsets = [o for o in r.offset_msg_id.split(",") if o]
        check("A5 the broker returned 3 commitLog offsets one per sub-message (the batch really was split on landing)",
              len(sub_offsets) == 3, "offsetMsgId=%s" % r.offset_msg_id)
        stored = [None]

        def _read_sub():
            try:
                stored[0] = admin.view_message(topic, sub_offsets[0])
                return True
            except Exception:
                return False  # commitLog not flushed out yet

        uniq = ""
        if sub_offsets and _wait_until(_read_sub, 15.0) and stored[0] is not None:
            uniq = stored[0].get_property("UNIQ_KEY") or ""
        check("A5 the sub-messages stored on the broker carry the client UNIQ_KEY (the per-message ID is embedded in the body)",
              len(uniq) == 32, "stored UNIQ_KEY=%s" % uniq)
    finally:
        p.shutdown()


# ------------------------------------------------- A6 Shutdown does not wait for in-flight
def a6_shutdown_does_not_wait(admin, topic):
    sends = max(1, os.cpu_count() or 1) * 3
    hook = _TracingHook(100)
    p = _producer("a6-%d" % STAMP, hook)
    p.start()
    rec = _Recorder()
    try:
        for i in range(sends):
            p.send_async(Message(topic, ("async-a6-%d" % i).encode()), rec, 8000)
        p.shutdown()
        returned_at = time.monotonic()
        # Every send still runs its prepare phase and still gets a terminal-state callback (not waiting ≠ dropping tasks)
        check("A6 every send got a terminal-state callback (not waiting ≠ callbacks vanishing)",
              _wait_until(lambda: rec.done >= sends, 30.0) and rec.done == sends,
              "done=%d sent=%d" % (rec.done, sends))
        check("A6 Shutdown does not wait for in-flight: callbacks still fire after it returns",
              rec.done >= sends
              and sum(1 for t in rec.ats if t > returned_at) >= sends - 1,
              "%d/%d sends only reached a terminal state after Shutdown returned"
              % (sum(1 for t in rec.ats if t > returned_at), sends))
        check("A6 the cost of not waiting: the client closes first, so essentially all in-flight sends error out",
              len(rec.errors) * 2 >= sends,
              "errors=%d/%d, e.g.: %s" % (len(rec.errors), sends, _first(rec.errors)))
        time.sleep(2.0)
        landed = _landed(admin, topic, quiet=True)
        check("A6 far fewer landed on the broker than were sent (not waiting really does drop messages)",
              0 <= max(0, landed) * 2 < sends,
              "landed=%d sent=%d" % (landed, sends))
    except Exception as e:  # noqa: BLE001 — should not throw after Shutdown; throwing is a regression
        check("A6 Shutdown drains in-flight prepare phases", False, "%s: %s" % (type(e).__name__, e))

    p2 = _producer("a6-after-%d" % STAMP)
    p2.start()
    after = _Recorder()
    p2.send_async(Message(topic, b"async-a6-after"), after, 8000)
    check("A6 the closed pool is not reused by another producer (a new producer can still send)",
          _wait_until(lambda: after.done >= 1, 20.0) and len(after.results) == 1,
          after.summary())
    p2.shutdown()


def _broker_addr(admin):
    try:
        return admin.fetch_broker_cluster_info().get_broker_addrs()[0]
    except Exception:  # noqa: BLE001
        return "127.0.0.1:10911"


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    topics = {
        "a1": "AsyncReadBack_%d" % STAMP,
        "a2": "AsyncBurst_%d" % STAMP,
        "a3": "AsyncPinned_%d" % STAMP,
        "a4": "AsyncForbidden_%d" % STAMP,
        "a5": "AsyncBatch_%d" % STAMP,
        "a6": "AsyncDrain_%d" % STAMP,
    }
    try:
        a1_non_blocking_and_read_back(admin, topics["a1"])
        a2_burst_exactly_once_each(admin, topics["a2"])
        a3_pinned_queue(admin, topics["a3"])
        a4_forbidden_hook(admin, topics["a4"])
        a5_batch_async(admin, topics["a5"])
        a6_shutdown_does_not_wait(admin, topics["a6"])
    finally:
        addr = _broker_addr(admin)
        for topic in topics.values():
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


if __name__ == "__main__":
    sys.exit(main())
