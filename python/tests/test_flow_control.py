# Why this must be locked offline: of the three per-queue gates, only the **count** one fires first in "small messages, fast pulls" scenarios;
# the other two fail in a **silent** direction when judged wrong:
#   - Queue-level byte gate failing ⇒ large messages blow up the pulled-but-unconsumed pile (real-world symptom: OOM / RT spike, no exception);
#   - Offset span gate failing ⇒ the head message keeps failing to consume while the rest piles up endlessly, and the offset span runs away;
#   - Topic-level division failing ⇒ the topic budget is never turned into a per-queue quota, so the instance total explodes while no single queue ever looks "over".
# The units have exactly one pitfall: the size threshold is **MiB**, not bytes. The comparison has one, too:
# every cache gate is **strictly greater than** the threshold, so a buffer that has reached it exactly still pulls.
#
# This file locks the offline semantics of flow-control judgment: the hit direction and units of the three per-queue
# gates, how the two topic knobs are divided into them when the queue assignment changes,
# plus the two stalls the gate can cause (the client's own cache threshold vs the broker's FLOW_CONTROL(215) reply).
# The POP path's cache gate (this queue's wait-ack debt against ``pop_threshold_for_queue``) is locked here too:
# it compares the same way, judges only its own queue, and counts on the **same** flow-control counter as the pull
# path, so one client-wide total and one warn cadence span both paths.
# For the real-machine side, "no message is lost after a gate hits", see ../verify_flow_control_live.py.
import inspect
import threading
import time

from client import consumer as consumer_mod
from client.consumer import (DefaultMQPushConsumer, PopProcessQueue,
                             FLOW_CONTROL_WARN_EVERY_N_TRIPS,
                             PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL,
                             PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL,
                             PULL_TIME_DELAY_WHEN_EXCEPTION,
                             is_flow_control_warn_trip)
from client.exception import MQBrokerException
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.codes import ResponseCode
from remoting.protocol.heartbeat import MessageModel

TOPIC = "FlowControlPyUnitTopic"
BROKER = "broker-a"
MIB = 1024 * 1024


def _queue(topic=TOPIC, queue_id=0):
    return MessageQueue(topic=topic, broker_name=BROKER, queue_id=queue_id)


def _sized(topic, store_size, queue_offset):
    m = MessageExt(topic=topic, body=b"x")
    m.broker_name = BROKER
    m.store_size = store_size
    m.queue_offset = queue_offset
    return m


def _n_sized(topic, store_size, n):
    return [_sized(topic, store_size, i) for i in range(n)]


def _consumer(group):
    """A not-yet-started consumer: _pending/_mq_map are pre-set by the test itself, no network touched."""
    return DefaultMQPushConsumer(group)


def _gates_off(c):
    """Turn off all gates except the threshold under test: an earlier hit would mask the branch under test."""
    c.pull_threshold_for_queue = 2 ** 31 - 1
    c.pull_threshold_size_for_queue = 0
    c.consume_concurrently_max_span = 0
    c.pull_threshold_for_topic = -1
    c.pull_threshold_size_for_topic = -1


def _stage(c, mq, msgs):
    """Pre-set a queue's buffer + register it in _mq_map, the table of owned queues with live pull state."""
    key = c._mq_key(mq)
    c._mq_map[key] = mq
    c._pending[key] = list(msgs)
    return key


def _own(c, count):
    """Register ``count`` owned queues with an empty buffer: that is the queue count the topic-level division divides by."""
    queues = [_queue(queue_id=i) for i in range(count)]
    for mq in queues:
        _stage(c, mq, [])
    return queues


def _hit(c, mq, key=None):
    return c._flow_control_hit(mq, key or c._mq_key(mq))


def test_defaults_only_the_count_gate_is_live():
    c = _consumer("G_fc_defaults")
    mq = _queue()
    # Defaults 1000 msgs / 100MiB / span 2000, topic level off: small buffers never hit
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert not _hit(c, mq)
    assert c._flow_control_triggered == 0
    # An empty buffer (nothing pulled yet) also does not hit, and the span must not be computed as negative
    _stage(c, mq, [])
    assert not _hit(c, mq)


def test_count_gate_fires_above_threshold_and_treats_zero_as_one():
    c = _consumer("G_fc_count")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 3
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    assert not _hit(c, mq)
    # The gate is strictly greater: reaching the threshold exactly still pulls.
    _stage(c, mq, _n_sized(TOPIC, 100, 3))
    assert not _hit(c, mq), "3 messages at threshold 3 must not hit (>, not >=)"
    _stage(c, mq, _n_sized(TOPIC, 100, 4))
    assert _hit(c, mq)
    assert c._flow_control_triggered == 1
    # threshold 0 is clamped to 1, which under ">" means "hit on the 2nd message" -- exactly what threshold 1 does.
    c.pull_threshold_for_queue = 0
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert not _hit(c, mq), "the max(1,n) guard must agree with threshold=1: one cached message is not over it"
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    assert _hit(c, mq), "pull_threshold_for_queue=0 must count as 1 message (the max(1,n) guard)"


def test_size_gate_is_in_mib_and_disabled_by_zero():
    c = _consumer("G_fc_size")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_size_for_queue = 1          # 1 MiB
    _stage(c, mq, _n_sized(TOPIC, 300, 3))       # 900 B
    assert not _hit(c, mq)
    # The unit is MiB, not bytes, and the gate is strictly greater: exactly 1 MiB does not hit.
    _stage(c, mq, [_sized(TOPIC, MIB, 0)])
    assert not _hit(c, mq), "exactly 1MiB must not hit (>, not >=)"
    # One byte over the boundary has to hit: this is what separates ">" from a sloppy "much greater".
    _stage(c, mq, [_sized(TOPIC, MIB + 1, 0)])
    assert _hit(c, mq), "1MiB+1B exceeds the 1MiB threshold"
    _stage(c, mq, _n_sized(TOPIC, 2 * MIB, 4))
    assert _hit(c, mq)
    # 0 = disable this gate; even a huge buffer is ignored
    c.pull_threshold_size_for_queue = 0
    assert not _hit(c, mq)


def test_span_gate_is_strictly_greater_and_measures_real_span():
    c = _consumer("G_fc_span")
    mq = _queue()
    _gates_off(c)
    c.consume_concurrently_max_span = 10
    _stage(c, mq, [_sized(TOPIC, 1, 0), _sized(TOPIC, 1, 100)])
    assert _hit(c, mq)
    c.consume_concurrently_max_span = 100
    assert not _hit(c, mq), "span 100 is not strictly greater than 100, so it must not hit"
    # An out-of-order buffer must also measure the real span (min/max, not the first-last difference)
    c.consume_concurrently_max_span = 10
    _stage(c, mq, [_sized(TOPIC, 1, 50), _sized(TOPIC, 1, 5), _sized(TOPIC, 1, 7)])
    assert _hit(c, mq), "an out-of-order buffer must measure the span as max-min, not the first-last difference"
    c.consume_concurrently_max_span = 0
    assert not _hit(c, mq)


# ---------------- The topic knobs are DIVIDED into the per-queue knobs, never summed at pull time ----------------
# A topic-level budget is not a pull-time gate: the consumer turns it into a per-queue quota
# (topic knob // the number of queues it owns) whenever the assignment changes, so the three per-queue
# checks stay the only place a queue is judged. The numerator is always the topic knob, which makes the
# division idempotent, and it floors at 1, because 0 would freeze every queue of the instance.

def test_assignment_change_divides_both_topic_knobs_into_the_per_queue_ones(caplog):
    c = _consumer("G_fc_topic_divide")
    _gates_off(c)
    # Per-queue values the user configured; the division overwrites them on purpose.
    c.pull_threshold_for_queue = 1000
    c.pull_threshold_size_for_queue = 100
    c.pull_threshold_for_topic = 1000
    c.pull_threshold_size_for_topic = 100
    _own(c, 4)
    with caplog.at_level("INFO", logger="rocketmq.client"):
        c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 250, "1000 messages over 4 owned queues is a 250 message quota"
    assert c.pull_threshold_size_for_queue == 25, "100MiB over 4 owned queues is a 25MiB quota"
    infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
    assert "The pullThresholdForQueue is changed from 1000 to 250" in infos, infos
    assert "The pullThresholdSizeForQueue is changed from 100 to 25" in infos, infos


def test_both_topic_knobs_off_leave_the_per_queue_knobs_untouched():
    """The paired control of the division: -1 means "this topic knob is off", so neither per-queue knob
    may move. Dividing unconditionally would rewrite a queue-level config the user never asked about."""
    c = _consumer("G_fc_topic_divide_off_control")
    _gates_off(c)
    c.pull_threshold_for_queue = 1000
    c.pull_threshold_size_for_queue = 100
    c.pull_threshold_for_topic = -1
    c.pull_threshold_size_for_topic = -1
    _own(c, 4)
    c._divide_topic_thresholds()
    assert (c.pull_threshold_for_queue, c.pull_threshold_size_for_queue) == (1000, 100)


def test_topic_knob_smaller_than_the_queue_count_floors_at_one():
    # 3 messages of topic budget across 4 queues is 0 by integer division; flooring at 1 is the only
    # thing that keeps the queue pulling at all -- a 0 threshold stops every queue of the instance.
    c = _consumer("G_fc_topic_divide_floor")
    _gates_off(c)
    c.pull_threshold_for_queue = 1000
    c.pull_threshold_size_for_queue = 100
    c.pull_threshold_for_topic = 3
    c.pull_threshold_size_for_topic = 3
    _own(c, 4)
    c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 1, "3 // 4 must floor at 1, never reach 0"
    assert c.pull_threshold_size_for_queue == 1, "3MiB // 4 must floor at 1, never reach 0"


def test_no_owned_queue_leaves_both_knobs_as_the_user_set_them():
    """The zero-queue guard: with nothing owned there is no divisor, so the per-queue knobs must stay put
    instead of collapsing to the floor value."""
    c = _consumer("G_fc_topic_divide_zero_queues")
    _gates_off(c)
    c.pull_threshold_for_queue = 1000
    c.pull_threshold_size_for_queue = 100
    c.pull_threshold_for_topic = 1000
    c.pull_threshold_size_for_topic = 100
    assert not c._mq_map, "this consumer owns no queue with live pull state"
    c._divide_topic_thresholds()
    assert (c.pull_threshold_for_queue, c.pull_threshold_size_for_queue) == (1000, 100)


def test_division_is_idempotent_and_always_divides_the_topic_knob():
    c = _consumer("G_fc_topic_divide_idempotent")
    _gates_off(c)
    c.pull_threshold_for_queue = 1000
    c.pull_threshold_size_for_topic = -1
    c.pull_threshold_for_topic = 1000
    _own(c, 4)
    c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 250
    # The same assignment again: the numerator is still the topic knob, so the answer cannot drift.
    c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 250, "re-dividing an unchanged assignment must not compound"
    # A change that doubles the owned queues re-derives from the TOPIC knob (1000 // 8 = 125),
    # not from the already divided per-queue value (250 // 8 = 31 would be the bug).
    _own(c, 8)
    c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 125, "the second change must divide the topic knob, not 250"


def test_gate_does_not_aggregate_the_sibling_queues():
    """The state that used to fire: 4 queues x 250 messages is exactly the 1000 message topic budget, so the
    old pull-time sibling sum paused every one of them. Under the division contract 250 is the per-queue
    quota and a queue at exactly its quota still pulls."""
    c = _consumer("G_fc_no_sibling_sum")
    _gates_off(c)
    c.pull_threshold_for_topic = 1000
    queues = _own(c, 4)
    for mq in queues:
        _stage(c, mq, _n_sized(TOPIC, 100, 250))
    c._divide_topic_thresholds()
    assert c.pull_threshold_for_queue == 250
    for mq in queues:
        assert not _hit(c, mq), "the instance-wide total must not pause a queue that is only at its own quota"
    assert c._flow_control_triggered == 0


def test_a_queue_over_its_divided_share_pauses_on_a_per_queue_reason_only(caplog):
    # Paired with the case above: one message over the divided quota still pauses that queue, and the
    # reason names only the per-queue count -- a leftover aggregation branch would say "topic".
    c = _consumer("G_fc_no_sibling_sum_over")
    _gates_off(c)
    c.pull_threshold_for_topic = 1000
    queues = _own(c, 4)
    for mq in queues:
        _stage(c, mq, _n_sized(TOPIC, 100, 250))
    c._divide_topic_thresholds()
    hot = queues[0]
    _stage(c, hot, _n_sized(TOPIC, 100, 251))
    with caplog.at_level("DEBUG", logger="rocketmq.client"):
        assert _hit(c, hot), "251 over a divided quota of 250 must pause"
    lines = [r.getMessage() for r in caplog.records
             if r.levelname == "DEBUG" and "flow control" in r.getMessage()]
    # The whole line, not just a substring: the reason field is exactly the per-queue count, with no
    # cross-queue accumulation wording left in it.
    assert lines == ["flow control: queue %s count=251, pause pull" % hot], lines


class _RouteOnlyClient(object):
    """Only the route view a rebalance round reads: the broadcasting path takes every queue of the topic."""

    def __init__(self):
        self.queues = []

    def get_topic_subscribe_info(self, topic):
        return list(self.queues)


def _rebalance_consumer(group):
    """A started consumer whose rebalance runs for real except for the network: parked pull loops, no revoke RPC."""
    c = DefaultMQPushConsumer(group)
    c._started = True
    c.use_heartbeat_v2 = False
    c.message_model = MessageModel.BROADCASTING
    c.subscription_data = {TOPIC: SubscriptionData(topic=TOPIC, sub_string="*")}
    c._queue_pull_loop = lambda mq: c._stop.wait(30)
    c._on_queues_revoked = lambda revoked: None
    client = _RouteOnlyClient()
    c._mq_client = client
    return c, client


def test_rebalance_divides_only_on_a_changed_assignment_and_after_the_state_update():
    c, client = _rebalance_consumer("G_fc_rebalance_divides")
    try:
        client.queues = [_queue(queue_id=i) for i in range(8)]
        # The start offset resolution is not what is under test; pre-filling it keeps the round local.
        for mq in client.queues:
            c._offset_table[c._mq_key(mq)] = 0
        c.pull_threshold_for_queue = 1000
        c.pull_threshold_size_for_queue = 100
        c.pull_threshold_for_topic = 1000
        c.pull_threshold_size_for_topic = 100
        client.queues = [_queue(queue_id=i) for i in range(4)]
        c._do_rebalance()
        assert (c.pull_threshold_for_queue, c.pull_threshold_size_for_queue) == (250, 25)
        # The very same allocation again: nothing changed, so a per-queue value edited since the last
        # round must survive -- dividing on every round would rewrite it on every rebalance cycle.
        c.pull_threshold_for_queue = 777
        c._do_rebalance()
        assert c.pull_threshold_for_queue == 777, "an unchanged assignment must not re-run the division"
        # Growing to 8 queues divides by this round's 8, not by the 4 the previous round saw: the pull
        # state tables are only complete once the thread update has run, which is why the division is last.
        client.queues = [_queue(queue_id=i) for i in range(8)]
        c._do_rebalance()
        assert c.pull_threshold_for_queue == 125, "the divisor must be the queue count after the update"
        assert c.pull_threshold_size_for_queue == 12, "100MiB // 8 queues"
    finally:
        c._stop.set()


def test_one_hit_counts_one_tick_even_when_every_gate_matches():
    """The counter is an observability surface for "how many times flow control stopped things": even when all
    three per-queue gates hit at once, only one tick is added."""
    c = _consumer("G_fc_counter")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    c.pull_threshold_size_for_queue = 1
    c.consume_concurrently_max_span = 1
    # A buffer of 2 messages at 2MiB each with offsets spanning to 100: all three per-queue gates hit at once
    _stage(c, mq, [_sized(TOPIC, 2 * MIB, 0), _sized(TOPIC, 2 * MIB, 100)])
    assert _hit(c, mq)
    assert c._flow_control_triggered == 1


# ---------------- Broker FLOW_CONTROL(215) vs the generic pull/pop error backoff ----------------
# The client's own threshold pause and the broker's answer are two different stalls:
# 215 is transient producer-side throttling (retry in 20ms), every other broker error keeps the
# 500ms generic backoff. Folding both into one delay either hammers a throttled broker or needlessly
# stalls a healthy queue behind an 11x-longer wait.

def _drive_loop(monkeypatch, loop_attr, client_method, errors, c, mq, key, setup=None):
    """Run the real loop against a client that keeps failing; return the recorded backoffs.

    The loop stops once the scripted errors are exhausted (the last recorded backoff sets the
    stop flag), so the assertion gates on recorded state, not on a wall-clock window.
    """
    pending = list(errors)
    delays = []

    class _Client(object):
        def __getattr__(self, name):
            if name == client_method:
                def _fail(*a, **k):
                    raise pending.pop(0)
                return _fail
            raise AttributeError(name)

    real_time = time

    class _FakeTime(object):
        def __getattr__(self, name):
            return getattr(real_time, name)

        def sleep(self, seconds):
            delays.append(seconds)
            if not pending:
                c._stop.set()

    c._mq_client = _Client()
    if setup is not None:
        setup()
    monkeypatch.setattr(consumer_mod, "time", _FakeTime())
    th = threading.Thread(target=getattr(c, loop_attr), args=(mq,), daemon=True)
    c._queue_threads[key] = th
    th.start()
    deadline = real_time.time() + 10
    while th.is_alive() and real_time.time() < deadline:
        real_time.sleep(0.01)
    c._stop.set()
    th.join(timeout=5)
    assert not pending, "the scripted errors must all be consumed"
    return delays


def _error_loop_consumer(group):
    c = DefaultMQPushConsumer(group)
    c._started = True
    mq = _queue()
    key = c._mq_key(mq)
    c.subscription_data = {mq.topic: SubscriptionData(topic=mq.topic, sub_string="*")}
    c._offset_table[key] = 0
    return c, mq, key


def test_broker_flow_control_pull_retries_in_20ms_other_codes_keep_500ms(monkeypatch):
    assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
    assert PULL_TIME_DELAY_WHEN_EXCEPTION == 0.5
    c, mq, key = _error_loop_consumer("G_fc_broker_pull")
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy"),
                          MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")],
                         c, mq, key)
    # 215 gets the short branch, any other broker code keeps the generic backoff:
    # the two directions pin each other (reverting either branch turns this red).
    assert delays == [0.02, 0.5], delays


def test_broker_flow_control_pop_retries_in_20ms_other_codes_keep_500ms(monkeypatch):
    pq = PopProcessQueue()

    def _setup():
        c._pop_queues[key] = pq

    c, mq, key = _error_loop_consumer("G_fc_broker_pop")
    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message",
                         [MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy"),
                          MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")],
                         c, mq, key, setup=_setup)
    assert delays == [0.02, 0.5], delays


def test_plain_client_error_pull_loop_keeps_generic_backoff(monkeypatch):
    # Control without a broker code at all: a generic exception must not pick the 20ms branch.
    c, mq, key = _error_loop_consumer("G_fc_generic")
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [RuntimeError("socket reset"),
                          MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy")],
                         c, mq, key)
    assert delays == [0.5, 0.02], delays


# ---------------- The client's own cache threshold vs the broker's 215 reply ----------------
# Both stall the pull, for opposite reasons: the local buffer is full (wait for the listener to
# drain it, 50ms) vs the broker is throttling (short retry, 20ms). Folding them into one delay, or
# leaving the local one longer than the drain interval, either stalls a healthy queue or spins.

def test_cache_flow_control_backoff_is_50ms_and_distinct_from_broker_215():
    assert PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL == 0.05
    assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
    assert PULL_TIME_DELAY_WHEN_EXCEPTION == 0.5
    # The pull-loop stall must actually use the constant, not a literal of its own.
    src = inspect.getsource(DefaultMQPushConsumer._queue_pull_loop)
    assert "PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL" in src
    assert "time.sleep(0.1)" not in src


def test_full_cache_pauses_the_pull_for_50ms_without_touching_the_broker(monkeypatch):
    c, mq, key = _error_loop_consumer("G_fc_cache_pause")

    def _setup():
        _gates_off(c)
        c.pull_threshold_for_queue = 1
        _stage(c, mq, _n_sized(TOPIC, 100, 2))   # 2 messages over a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message", [], c, mq, key, setup=_setup)
    assert delays == [PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL], delays
    assert c._flow_control_triggered == 1


def test_buffer_at_the_threshold_pulls_instead_of_stalling(monkeypatch):
    # Paired control: one message exactly AT the threshold must not pause, so the loop reaches the
    # broker and records the *error* backoff instead of the cache one.
    c, mq, key = _error_loop_consumer("G_fc_cache_pass")

    def _setup():
        _gates_off(c)
        c.pull_threshold_for_queue = 1
        _stage(c, mq, _n_sized(TOPIC, 100, 1))   # 1 message at a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [RuntimeError("socket reset")], c, mq, key, setup=_setup)
    assert delays == [PULL_TIME_DELAY_WHEN_EXCEPTION], delays
    assert c._flow_control_triggered == 0


# ---------------- The warn cadence ----------------
# A throttled queue trips the gate on every pull round. Warning every round buries the message in
# noise; warning never loses the only signal that the buffer is full. So: one warn per 1000 trips.

def test_warn_cadence_is_the_first_trip_and_every_1000th_after():
    assert FLOW_CONTROL_WARN_EVERY_N_TRIPS == 1000
    assert is_flow_control_warn_trip(1)
    assert is_flow_control_warn_trip(1001)
    assert is_flow_control_warn_trip(2001)
    assert not is_flow_control_warn_trip(2)
    assert not is_flow_control_warn_trip(1000)
    assert not is_flow_control_warn_trip(1002)


def _warns(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelname == "WARNING" and "flow control" in r.getMessage()]


def test_gate_warns_on_the_first_trip_and_stays_quiet_on_the_next(caplog):
    c = _consumer("G_fc_warn_cadence")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    with caplog.at_level("WARNING"):
        assert _hit(c, mq)
        assert len(_warns(caplog)) == 1, "the 1st trip must warn"
        assert _hit(c, mq)
        assert len(_warns(caplog)) == 1, "the 2nd trip must stay on the debug channel only"
        # Jump to the boundary and warn again: this pins the modulo, not "warn once and stop".
        c._flow_control_triggered = FLOW_CONTROL_WARN_EVERY_N_TRIPS
        assert _hit(c, mq)
        assert len(_warns(caplog)) == 2, "trip 1001 must warn again"
    assert "pause pull for 50 ms" in _warns(caplog)[0]


def test_gate_debug_records_every_trip(caplog):
    # The debug channel keeps the per-trip detail; only the warn is rate-limited.
    c = _consumer("G_fc_debug")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    with caplog.at_level("DEBUG", logger="rocketmq.client"):
        for _ in range(3):
            assert _hit(c, mq)
        debugs = [r.getMessage() for r in caplog.records
                  if r.levelname == "DEBUG" and "flow control" in r.getMessage()]
    assert len(debugs) == 3
    assert c._flow_control_triggered == 3


# ---------------- The POP path's cache gate: this queue's wait-ack debt ----------------
# POP has no "pulled but not consumed" buffer, so the wait-ack count is the only cache gate on that
# path. It must compare the same way (strictly greater), judge only its own queue's debt, and -- the
# point of this section -- land on the SAME _flow_control_triggered counter as the pull path, so one
# client-wide flow-control total and one warn cadence cover both paths.

def _pop_debt(count):
    """A queue state carrying exactly ``count`` popped-but-unacked messages."""
    pq = PopProcessQueue()
    if count:
        pq.inc_found_msg(count)
    return pq


def _pop_hit(c, mq, debt):
    return c._pop_flow_control_hit(mq, _pop_debt(debt))


def test_pop_gate_is_strictly_greater_at_the_default_threshold():
    """Default 96: exactly 96 unacked messages still pops, 97 trips -- the same ">" as every cache gate."""
    c = _consumer("G_fc_pop_boundary")
    mq = _queue()
    assert c.pop_threshold_for_queue == 96
    assert not _pop_hit(c, mq, 0), "nothing popped and unacked must not hit"
    assert not _pop_hit(c, mq, 96), "96 at threshold 96 must not hit (>, not >=)"
    assert c._flow_control_triggered == 0, "a non-hit must not count"
    assert _pop_hit(c, mq, 97), "threshold+1 must hit"
    assert c._flow_control_triggered == 1
    # The default is a config, not a constant of the gate: a tightened threshold follows it.
    c.pop_threshold_for_queue = 3
    assert not _pop_hit(c, mq, 3)
    assert _pop_hit(c, mq, 4)
    assert c._flow_control_triggered == 2


def test_pop_gate_reads_only_its_own_queue_debt():
    """No sibling summing on the POP path either: one queue over the threshold trips, its empty neighbour keeps popping."""
    c = _consumer("G_fc_pop_no_sibling_sum")
    c.pop_threshold_for_queue = 3
    hot = _queue(queue_id=0)
    cold = _queue(queue_id=1)
    assert _pop_hit(c, hot, 4)
    assert not _pop_hit(c, cold, 0), "an empty sibling must not be paused by the hot queue's debt"
    # Three more queues each at exactly the threshold: 4 x 3 = 12 of instance-wide debt, far above 3,
    # yet each queue sits at its own threshold, so nothing pauses.
    for qid in range(1, 4):
        assert not _pop_hit(c, _queue(queue_id=qid), 3)
    assert c._flow_control_triggered == 1


def test_pop_and_pull_trips_share_one_flow_control_counter():
    """One pull trip plus one POP trip on the same consumer is two hits of ONE counter.

    This is the assertion that goes red when the POP gate is a silent branch of its own.
    """
    c = _consumer("G_fc_pop_shared_counter")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    c.pop_threshold_for_queue = 1
    _stage(c, mq, _n_sized(TOPIC, 100, 2))    # 2 cached over a pull threshold of 1
    assert _hit(c, mq)
    assert _pop_hit(c, mq, 2)                 # 2 unacked over a pop threshold of 1
    assert c._flow_control_triggered == 2, "the POP trip must be counted on the pull path's counter"
    # Paired control: a POP-only hit on a fresh consumer counts exactly one.
    only_pop = _consumer("G_fc_pop_shared_counter_control")
    only_pop.pop_threshold_for_queue = 1
    assert _pop_hit(only_pop, mq, 2)
    assert only_pop._flow_control_triggered == 1
    # And a below-threshold POP debt adds nothing to a counter already at 2.
    assert not _pop_hit(c, mq, 1)
    assert c._flow_control_triggered == 2


def test_pop_gate_warn_cadence_follows_the_shared_counter(caplog):
    """The warn cadence counts POP trips too: trip 1 warns, trip 2 and 3 (one of them a pull trip) stay quiet,
    trip 1001 warns again. The 1000 trips in between are set, not run."""
    assert is_flow_control_warn_trip(1)
    assert not is_flow_control_warn_trip(2)
    assert not is_flow_control_warn_trip(1000)
    assert is_flow_control_warn_trip(1001)
    c = _consumer("G_fc_pop_warn_cadence")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    c.pop_threshold_for_queue = 1
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    with caplog.at_level("WARNING"):
        assert _pop_hit(c, mq, 2)
        assert len(_warns(caplog)) == 1, "the 1st trip must warn"
        assert _pop_hit(c, mq, 2)
        assert len(_warns(caplog)) == 1, "the 2nd trip must stay on the debug channel only"
        assert _hit(c, mq), "a pull trip joins the same count"
        assert len(_warns(caplog)) == 1, "trip 3 (the pull path's) must not warn either"
        assert c._flow_control_triggered == 3
        c._flow_control_triggered = FLOW_CONTROL_WARN_EVERY_N_TRIPS
        assert _pop_hit(c, mq, 2)
        assert len(_warns(caplog)) == 2, "trip 1001 must warn again"
    first = _warns(caplog)[0]
    assert "wait_ack=2" in first, first
    assert "pop_threshold_for_queue=1" in first, first
    assert "pause pop for 50 ms" in first, first
    assert "flowControlTimes=1" in first, first


def test_pop_gate_debug_records_the_debt_and_the_threshold(caplog):
    """The per-trip debug line names the queue, its own wait-ack count and the threshold it exceeded."""
    c = _consumer("G_fc_pop_debug")
    mq = _queue()
    c.pop_threshold_for_queue = 2
    with caplog.at_level("DEBUG", logger="rocketmq.client"):
        assert not _pop_hit(c, mq, 2)
        assert _pop_hit(c, mq, 3)
    debugs = [r.getMessage() for r in caplog.records
              if r.levelname == "DEBUG" and "flow control" in r.getMessage()]
    assert debugs == ["flow control: queue %s wait_ack=3 over pop_threshold_for_queue=2, pause pop" % mq], debugs


def test_pop_cache_flow_control_pauses_the_pop_for_50ms_without_touching_the_broker(monkeypatch):
    c, mq, key = _error_loop_consumer("G_fc_pop_cache_pause")

    def _setup():
        c.pop_threshold_for_queue = 1
        c._pop_queues[key] = _pop_debt(2)      # 2 popped-but-unacked over a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message", [], c, mq, key, setup=_setup)
    assert delays == [PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL], delays
    assert c._flow_control_triggered == 1, "the loop's pause must be the counted trip"
    assert c._pop_queues[key].wait_ack_count() == 2, "the gate only pauses popping, it never changes the debt"


def test_pop_debt_at_the_threshold_pops_instead_of_stalling(monkeypatch):
    # Paired control of the case above: debt exactly AT the threshold must not pause, so the loop
    # reaches the broker and records the *error* backoff instead of the cache one.
    c, mq, key = _error_loop_consumer("G_fc_pop_cache_pass")

    def _setup():
        c.pop_threshold_for_queue = 1
        c._pop_queues[key] = _pop_debt(1)      # 1 message at a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message",
                         [RuntimeError("socket reset")], c, mq, key, setup=_setup)
    assert delays == [PULL_TIME_DELAY_WHEN_EXCEPTION], delays
    assert c._flow_control_triggered == 0

