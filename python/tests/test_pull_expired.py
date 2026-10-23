# Offline locking of stalled-queue removal and rebuild; real-machine recovery scenarios are in ../verify_pull_expired_live.py
# (inject a stall -> go through the production rebalance path to rebuild -> the same queue keeps consuming, offsets do not regress).
import threading
import time

from client.consumer import (MIN_POP_INVISIBLE_TIME, DefaultMQPushConsumer,
                                      PopProcessQueue, PULL_MAX_IDLE_TIME)
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.heartbeat import MessageModel

GROUP = "G_pull_expired"
STALE = PULL_MAX_IDLE_TIME + 5.0


def _sub():
    return SubscriptionData(topic="TopicTest", sub_string="*")


def _msgs(n):
    out = []
    for i in range(n):
        m = MessageExt(topic="TopicTest", body=b"x")
        m.queue_offset = i
        out.append(m)
    return out


def _consumer(pop_mode=False):
    """A "already running, assigned 1 queue" consumer: the pull loop is swapped for a controllable empty shell.

    Swapping target to park (just wait), the thread stays alive but **never touches the network**, so
    _rebalance_pull_threads's revoke/build branches can be verified separately; stamping is written by the test itself via
    _last_pull_table, equivalent to what the real loop does at each round's entry.
    """
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    c.pop_mode = pop_mode
    mq = MessageQueue("TopicTest", "broker-a", 0)
    c._assigned = [mq]
    key = c._mq_key(mq)
    c._queue_pull_loop = lambda m: c._stop.wait(30)      # no network access, just occupies the thread
    c._queue_pop_loop = lambda m: c._stop.wait(30)
    return c, mq, key


def _parked_thread(c):
    t = threading.Thread(target=lambda: c._stop.wait(30), daemon=True)
    t.start()
    return t


def _dead_thread():
    t = threading.Thread(target=lambda: None, daemon=True)
    t.start()
    t.join()
    return t


def _retire_log(c):
    """Swap _on_queues_revoked for a recorder: the wrap-up actions at revocation are the point of the assertions, but no real RPC may be sent."""
    seen = []
    c._on_queues_revoked = lambda revoked: seen.extend(revoked)
    return seen


# ---------------- Constants and criteria ----------------

def test_pull_max_idle_time_matches_the_long_polling_default():
    pass
    assert PULL_MAX_IDLE_TIME == 120.0


def test_stall_predicate_needs_both_signals():
    """The criterion: a dead thread counts as stalled; a live thread that has not stamped past the threshold also counts; a new loop that has never stamped does not."""
    c, mq, key = _consumer()
    try:
        c._queue_threads[key] = _parked_thread(c)
        assert c._pull_stalled_locked(key) is False, "a just-started loop must not be judged stalled"

        c._last_pull_table[key] = time.time() - (PULL_MAX_IDLE_TIME - 10)
        assert c._pull_stalled_locked(key) is False, "within the threshold it must not be revoked"

        c._last_pull_table[key] = time.time() - STALE
        assert c._pull_stalled_locked(key) is True, "past the threshold it must be judged stalled"

        c._queue_threads[key] = _dead_thread()
        c._last_pull_table[key] = time.time()
        assert c._pull_stalled_locked(key) is True, "a thread that has exited is immediately judged stalled, no need to wait out the threshold"
    finally:
        c._stop.set()


def test_stall_predicate_ignores_threads_not_yet_stampable():
    """The thread is built but has not yet reached the stamping point (no timestamp in the table): not stalled, otherwise it would be revoked right after assignment."""
    c, mq, key = _consumer()
    try:
        c._queue_threads[key] = _parked_thread(c)
        assert key not in c._last_pull_table
        assert c._pull_stalled_locked(key) is False
    finally:
        c._stop.set()


# ---------------- Revoke + build: recovery in the same pass ----------------

def test_stalled_queue_is_retired_and_rebuilt_in_one_pass():
    """The core scenario: the stalled queue is revoked (offsets persisted, buffer dropped), and a new thread rebuilds it in the same pass."""
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        old = _parked_thread(c)
        c._queue_threads[key] = old
        c._last_pull_table[key] = time.time() - STALE
        c._mq_map[key] = mq
        c._pending[key] = _msgs(2)              # a batch pulled but not consumed
        c._offset_table[key] = 77                 # pull cursor
        c._consume_offsets[key] = 42              # consumed offset, must be persisted

        c._rebalance_pull_threads()

        assert retired == [(mq, 42)], "a stalled queue must be wrapped up per the revocation: persist its consumed offset"
        new = c._queue_threads[key]
        assert new is not old, "the stalled thread must be swapped out, otherwise it is still dead"
        assert c._pending.get(key) is None, "the pulled-but-unconsumed buffer must be dropped and redelivered by the new loop"
        assert c._offset_table.get(key) is None, "the pull cursor must be dropped (the new loop recomputes from the broker offset)"
        assert c._consume_offsets.get(key) is None, "the consumed offset is handed to the broker, not kept locally"
        assert 0 <= time.time() - c._last_pull_table[key] < 5, "re-claim the slot at rebuild time, do not inherit the old timestamp"
    finally:
        c._stop.set()


def test_thread_death_is_enough_to_rebuild():
    pass
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        old = _dead_thread()
        c._queue_threads[key] = old
        c._last_pull_table[key] = time.time()     # the stamp is still fresh, only the thread died
        c._mq_map[key] = mq

        c._rebalance_pull_threads()

        assert c._queue_threads[key] is not old
        assert len(retired) == 1 and retired[0][0] == mq
    finally:
        c._stop.set()


def test_healthy_queue_is_left_alone():
    """If it is not stalled it must not be touched: swapping threads means dropping and redelivering in-flight messages, adding duplicate consumption for nothing."""
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        t = _parked_thread(c)
        c._queue_threads[key] = t
        c._last_pull_table[key] = time.time()
        c._mq_map[key] = mq
        c._pending[key] = ["m"]

        c._rebalance_pull_threads()

        assert c._queue_threads[key] is t
        assert c._pending[key] == ["m"]
        assert retired == []
    finally:
        c._stop.set()


def test_assignment_registers_in_mq_map_before_first_message():
    pass
    c, mq, key = _consumer()
    _retire_log(c)
    try:
        c._rebalance_pull_threads()
        assert c._mq_map[key] == mq
    finally:
        c._stop.set()


def test_pop_expired_uses_last_pop_timestamp_and_drops_pq():
    """POP branch: the criterion reads lastPopTimestamp (PopProcessQueue:74-76), and set_dropped at revocation."""
    c, mq, key = _consumer(pop_mode=True)
    retired = _retire_log(c)
    try:
        old = _parked_thread(c)
        pq = PopProcessQueue()
        c._queue_threads[key] = old
        c._pop_queues[key] = pq
        c._mq_map[key] = mq
        c._last_pull_table[key] = time.time() - STALE

        c._rebalance_pull_threads()

        assert pq.is_dropped() is True, "in-flight batches must stop: neither consume nor ack, waiting for the broker to come back"
        assert c._pop_queues[key] is not pq, "the rebuild must swap in a clean PopProcessQueue"
        assert len(retired) == 1 and retired[0][0] == mq
    finally:
        c._stop.set()


def test_stopped_consumer_is_not_swept():
    """During shutdown threads exit one after another anyway: no stall is judged at this point, otherwise it would spam a bunch of fake [BUG] logs."""
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        c._started = False
        old = _dead_thread()
        c._queue_threads[key] = old
        c._mq_map[key] = mq
        c._consume_offsets[key] = 7

        c._rebalance_pull_threads()

        assert c._queue_threads[key] is old
        assert retired == [], "a shutdown must not be treated as stall self-healing"
        assert c._consume_offsets[key] == 7
    finally:
        c._stop.set()


# ---------------- Stamping timing: stamp on initiation ----------------

def test_pull_loop_stamps_before_lock_and_flow_control():
    pass
    c, mq, key = _consumer()
    pulls = []

    class _Client(object):
        def pull_message(self, *args, **kwargs):
            pulls.append(1)
            raise AssertionError("must not reach the network")

    c._mq_client = _Client()
    c.subscription_data = {mq.topic: _sub()}
    del c._queue_pull_loop                      # use the real loop, not _consumer()'s empty shell
    c.pull_threshold_for_queue = 1
    c._pending[key] = _msgs(3)                  # 3 pulled, above threshold 1 -> flow control triggers every round
    c._offset_table[key] = 0
    try:
        th = threading.Thread(target=c._queue_pull_loop, args=(mq,), daemon=True)
        c._queue_threads[key] = th              # the loop recognizes the owner via _owns_queue, so register first, then start
        th.start()
        deadline = time.time() + 3
        while time.time() < deadline and key not in c._last_pull_table:
            time.sleep(0.05)
        c._stop.set()
        th.join(timeout=3)
        assert key in c._last_pull_table, "stamping must happen even during flow control"
        assert pulls == [], "when flow control triggers, no network access"
        assert len(c._pending[key]) == 3, "flow control only pauses pulling, leaving the buffer alone"
    finally:
        c._stop.set()


def test_pop_loop_stamps_before_flow_control():
    """POP is the same: the entry updates both _last_pull_table and PopProcessQueue.lastPopTimestamp."""
    c, mq, key = _consumer(pop_mode=True)
    pops = []

    class _Client(object):
        def pop_message(self, *args, **kwargs):
            pops.append(1)
            raise AssertionError("must not reach the network")

    c._mq_client = _Client()
    c.subscription_data = {mq.topic: _sub()}
    del c._queue_pop_loop
    pq = PopProcessQueue()
    pq.inc_found_msg(5)
    c._pop_queues[key] = pq
    c.pop_threshold_for_queue = 1               # waitAck 5 > 1 -> flow control triggers every round
    c.pop_invisible_time = MIN_POP_INVISIBLE_TIME
    old_stamp = pq.last_pop_timestamp
    try:
        th = threading.Thread(target=c._queue_pop_loop, args=(mq,), daemon=True)
        c._queue_threads[key] = th
        th.start()
        deadline = time.time() + 3
        while time.time() < deadline and pq.last_pop_timestamp <= old_stamp:
            time.sleep(0.05)
        c._stop.set()
        th.join(timeout=3)
        assert pq.last_pop_timestamp > old_stamp, "lastPopTimestamp must be updated even when pops cannot go out"
        assert key in c._last_pull_table
        assert pops == []
    finally:
        c._stop.set()


# ---------------- Running info: 307 must be able to see the timestamps ----------------

def test_running_info_reports_real_last_pull_timestamp():
    pass
    c, mq, key = _consumer()
    _retire_log(c)
    try:
        c.name_server_addrs = ["127.0.0.1:9876"]
        c.namespace = None
        c._mq_map[key] = mq
        c._consume_offsets[key] = 42
        moment = time.time() - 30
        c._last_pull_table[key] = moment

        pqi = list(c.consumer_running_info().mq_table.values())[0]
        assert pqi["lastPullTimestamp"] == int(moment * 1000)
        assert pqi["commitOffset"] == 42
    finally:
        c._stop.set()


def test_running_info_pop_table_reports_last_pop_timestamp():
    from client.consumer import MessageModel

    c, mq, key = _consumer(pop_mode=True)
    _retire_log(c)
    try:
        assert c.message_model == MessageModel.CLUSTERING
        c.name_server_addrs = ["127.0.0.1:9876"]
        c.namespace = None
        c._mq_map[key] = mq
        pq = PopProcessQueue()
        pq.last_pop_timestamp = time.time() - 15
        pq.inc_found_msg(3)
        c._pop_queues[key] = pq

        entry = list(c.consumer_running_info().mq_pop_table.values())[0]
        assert entry["lastPullTimestamp"] == int(pq.last_pop_timestamp * 1000)
        assert entry["cachedMsgCount"] == 3
        assert entry["droped"] is False
    finally:
        c._stop.set()
