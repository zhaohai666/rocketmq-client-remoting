# The gate that locks suspend/resume offline: after suspend() fetching and POP stop, after
# resume() they restart, and timestamps must keep advancing during suspension (otherwise the
# stall sweeper would mistake it for a dead loop).
import threading
import time

from client.consumer import (DefaultMQPushConsumer, PopProcessQueue,
                             PULL_MAX_IDLE_TIME, PULL_TIME_DELAY_WHEN_SUSPEND)
from client.consumer_result import PopResult, PullResult, PullStatus
from common.message import MessageQueue
from common.subscription_data import SubscriptionData

GROUP = "G_suspend"
TOPIC = "SuspendPyUnitTopic"
BROKER = "broker-a"


class _FakeClient(object):
    """A broker stand-in that only records: every reply is "no new message", never really delivering.

    Recording uses the **initiation timestamp** rather than a count, so the same assertion can ask
    both "are there still new requests after suspension" and "does the stamping keep advancing
    during suspension".

    Every reply sleeps LONG_POLL: a real broker's long poll would hold this request, and a
    stand-in that does not slow down becomes tens of thousands of no-op spins per second -- the
    few-millisecond window between suspend() and "taking the baseline" would squeeze in hundreds
    of pulls, and the assertion would turn into a race. At 30ms per reply that window admits at
    most 1 request, and it is already absorbed by the settle before taking the baseline.
    """

    LONG_POLL = 0.03

    def __init__(self):
        self.pull_at = []
        self.pop_at = []

    def pull_message(self, group, mq, offset, *args, **kwargs):
        time.sleep(self.LONG_POLL)
        self.pull_at.append(time.time())
        return PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=offset + 1)

    def pop_message(self, *args, **kwargs):
        time.sleep(self.LONG_POLL)
        self.pop_at.append(time.time())
        return PopResult(PopStatus.POLLING_NOT_FOUND)

    def count(self):
        """The number of requests this stand-in has seen: the pull/pop paths each keep their own
        list, so the sum is the total."""
        return len(self.pull_at) + len(self.pop_at)


def _consumer(pop_mode=False):
    """A "started, assigned 1 queue" consumer with a recording stand-in, never touching the real network."""
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    c.pop_mode = pop_mode
    fake = _FakeClient()
    c._mq_client = fake
    mq = MessageQueue(TOPIC, BROKER, 0)
    c._assigned = [mq]
    c.subscription_data = {TOPIC: SubscriptionData(topic=TOPIC, sub_string="*")}
    key = c._mq_key(mq)
    c._offset_table[key] = 0          # offset known, skipping _resolve_initial_offset
    c._mq_map[key] = mq
    return c, mq, key, fake


def _run_loop(c, mq, key, pop_mode):
    """Start a real loop thread (_owns_queue recognizes the owner via the thread table, so register first, then start)."""
    target = c._queue_pop_loop if pop_mode else c._queue_pull_loop
    th = threading.Thread(target=target, args=(mq,), daemon=True)
    c._queue_threads[key] = th
    th.start()
    return th


def _wait_pulls(fake, at_least, timeout=3.0):
    """Wait until the stand-in has seen at_least requests (shared by the pull/pop paths)."""
    deadline = time.time() + timeout
    while time.time() < deadline and fake.count() < at_least:
        time.sleep(0.05)
    return fake.count() >= at_least


def _settle():
    pass
    time.sleep(0.5)


def _stop(c, th):
    c._stop.set()
    c._paused = False
    th.join(timeout=3)



def test_suspend_backoff_matches_the_default_interval():
    pass
    assert PULL_TIME_DELAY_WHEN_SUSPEND == 1.0


def test_suspend_flag_is_off_by_default():
    c, mq, key, fake = _consumer()
    assert c.is_paused() is False, "not suspended by default: pause starts as false"


# ---------------- Core: suspension stops pulls, resume restarts them ----------------

def test_suspend_stops_pulls_and_resume_restarts_them():
    """After suspension no more PULL_MESSAGE may go out; after resume it must re-initiate on its own (no thread rebuild needed)."""
    c, mq, key, fake = _consumer()
    th = _run_loop(c, mq, key, pop_mode=False)
    try:
        assert _wait_pulls(fake, 2), "without a few pulls first, 'stopped' is unconvincing"
        c.suspend()
        assert c.is_paused() is True
        _settle()                     # let the in-flight request issued before suspension land first

        baseline = fake.count()
        stamp_before = c._last_pull_table[key]
        time.sleep(2.5)

        assert fake.count() == baseline, "no new PULL_MESSAGE may go out during suspension"
        assert c._last_pull_table[key] > stamp_before, \
            "lastPullTimestamp must keep advancing during suspension: the gate stamps after the check," \
            "otherwise a suspension longer than 120s would be treated as a dead loop by the stall criterion, and rebalance would revoke the whole assignment"
        with c._lock:
            assert c._pull_stalled_locked(key) is False, "suspension is not a stall"
        assert c._queue_threads[key] is th, "suspension does not swap threads or revoke queues: it only flips one boolean"
        assert c._mq_map.get(key) == mq, "the assignment stays as-is"

        c.resume()
        assert c.is_paused() is False
        assert _wait_pulls(fake, baseline + 1, timeout=5.0), "after resume it must restart pulling on its own"
    finally:
        _stop(c, th)


def test_suspend_leaves_already_fetched_state_alone():
    pass
    c, mq, key, fake = _consumer()
    from common.message import MessageExt
    buffered = [MessageExt(topic=TOPIC, body=b"x")]
    c._pending[key] = list(buffered)
    c._offset_table[key] = 17
    th = _run_loop(c, mq, key, pop_mode=False)
    try:
        c.suspend()
        _settle()                              # the request issued before suspension has already landed
        cursor_before = c._offset_table[key]
        time.sleep(1.2)
        assert list(c._pending[key]) == buffered, "messages already fetched locally stay in the buffer as usual"
        assert c._offset_table[key] == cursor_before, "suspension does not change the pull cursor"
    finally:
        _stop(c, th)


def test_resume_triggers_rebalance():
    pass
    c, mq, key, fake = _consumer()
    try:
        c.suspend()
        c._rebalance_now.clear()
        c.resume()
        assert c._rebalance_now.is_set() is True, "resume must wake the rebalance loop"
    finally:
        c._paused = False


# ---------------- POP mode: the same gate ----------------

def test_pop_mode_suspend_stops_pops_and_keeps_stamping():
    pass
    c, mq, key, fake = _consumer(pop_mode=True)
    pq = PopProcessQueue()
    c._pop_queues[key] = pq
    c.pop_threshold_for_queue = 1000
    th = _run_loop(c, mq, key, pop_mode=True)
    try:
        assert _wait_pulls(fake, 2), "without a few pops first, 'stopped' is unconvincing"
        c.suspend()
        _settle()                              # let the in-flight request land first

        baseline = fake.count()
        stamp_before = pq.last_pop_timestamp
        table_before = c._last_pull_table[key]
        time.sleep(2.5)

        assert fake.count() == baseline, "no new POP_MESSAGE may go out during suspension"
        assert pq.last_pop_timestamp > stamp_before, "lastPopTimestamp must keep advancing"
        assert c._last_pull_table[key] > table_before
        with c._lock:
            assert c._pull_stalled_locked(key) is False, "suspension is not a stall, the 120s criterion should not hit"
        assert pq.is_dropped() is False, "suspension does not revoke queues"

        c.resume()
        assert _wait_pulls(fake, baseline + 1, timeout=5.0), "after resume it must restart popping on its own"
    finally:
        _stop(c, th)


# ---------------- Idempotence / safe before start ----------------

def test_suspend_and_resume_are_idempotent_and_safe_before_start():
    pass
    c = DefaultMQPushConsumer("G_suspend_idle")
    assert c.is_paused() is False
    c.suspend()
    c.suspend()
    assert c.is_paused() is True, "a repeated suspend must stay suspended"
    c.resume()
    c.resume()
    assert c.is_paused() is False, "a repeated resume must stay running"


def test_paused_consumer_is_not_swept_by_rebalance():
    """Look at the two criteria together: a suspended queue must stay **untouched** in rebalance.

    This is the combination most easily hit on a real machine: after suspend() operators usually
    walk away for a while and come back to resume(), and if rebalance revoked the queue in the
    meantime, resume would see redelivered messages.
    """
    c, mq, key, fake = _consumer()
    retired = []
    c._on_queues_revoked = lambda revoked: retired.extend(revoked)
    th = _run_loop(c, mq, key, pop_mode=False)
    try:
        assert _wait_pulls(fake, 1)
        c.suspend()
        assert c._pull_stalled_locked(key) is False
        c._rebalance_pull_threads()
        assert retired == [], "a suspended queue must not be revoked (revoking persists offsets and drops the buffer)"
        assert c._queue_threads[key] is th
    finally:
        _stop(c, th)


def test_stall_window_is_far_longer_than_the_suspend_backoff():
    """The relative relationship of the two constants: 1s backoff vs 120s stall threshold.

    Suspension survives the stall criterion by "stamping once every 1s"; conversely, if the
    backoff were written as 150s, suspension = stall, and that assertion in the first test
    would no longer hold the real scenario.
    """
    assert PULL_TIME_DELAY_WHEN_SUSPEND < PULL_MAX_IDLE_TIME / 2
    assert PULL_MAX_IDLE_TIME == 120.0
