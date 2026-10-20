# Why this must be locked offline: of the five gates, only the **count** one fires first in "small messages, fast pulls" scenarios;
# the other four fail in a **silent** direction when judged wrong:
#   - Queue-level byte gate failing ⇒ large messages blow up the pulled-but-unconsumed pile (real-world symptom: OOM / RT spike, no exception);
#   - Offset span gate failing ⇒ the head message keeps failing to consume while the rest piles up endlessly, and the offset span runs away;
#   - Topic-level gate failing ⇒ multiple queues of the same topic act independently; no single queue is "over" but the instance total explodes.
# The units have exactly one pitfall: the size threshold is **MiB**, not bytes; the span is **strictly greater** while count/bytes are **>=**.
#
# This file locks the offline semantics of flow-control judgment: the hit direction and units of the five gates are pinned one by one.
# For the real-machine side, "no message is lost after a gate hits", see ../verify_flow_control_live.py.
import threading
import time

from client import consumer as consumer_mod
from client.consumer import (DefaultMQPushConsumer, PopProcessQueue,
                             PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL,
                             PULL_TIME_DELAY_WHEN_EXCEPTION)
from client.exception import MQBrokerException
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.codes import ResponseCode

TOPIC = "FlowControlPyUnitTopic"
OTHER_TOPIC = "FlowControlPyUnitOther"
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
    """Pre-set a queue's buffer + register it as assigned (topic-level thresholds aggregate sibling queues via _mq_map)."""
    key = c._mq_key(mq)
    c._mq_map[key] = mq
    c._pending[key] = list(msgs)
    return key


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


def test_count_gate_fires_at_threshold_and_treats_zero_as_one():
    c = _consumer("G_fc_count")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 3
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    assert not _hit(c, mq)
    _stage(c, mq, _n_sized(TOPIC, 100, 3))
    assert _hit(c, mq)
    assert c._flow_control_triggered == 1
    c.pull_threshold_for_queue = 0
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert _hit(c, mq), "pull_threshold_for_queue=0 must count as 1 message (the max(1,n) guard)"


def test_size_gate_is_in_mib_and_disabled_by_zero():
    c = _consumer("G_fc_size")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_size_for_queue = 1          # 1 MiB
    _stage(c, mq, _n_sized(TOPIC, 300, 3))       # 900 B
    assert not _hit(c, mq)
    # The unit is MiB, not bytes: exactly 1 MiB counts as a hit (>=)
    _stage(c, mq, [_sized(TOPIC, MIB, 0)])
    assert _hit(c, mq), "exactly 1MiB must hit (>=, not >)"
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


def test_topic_count_gate_aggregates_siblings_but_not_other_topics():
    c = _consumer("G_fc_topic_count")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    other = _queue(topic=OTHER_TOPIC)
    _gates_off(c)
    c.pull_threshold_for_topic = 2
    # 1 message in a single queue: the topic level also sees only 1
    _stage(c, q0, _n_sized(TOPIC, 1, 1))
    assert not _hit(c, q0)
    # Sibling queues of the same topic with 1 message each => 2 cumulative, both queues must stop
    k1 = _stage(c, q1, _n_sized(TOPIC, 1, 1))
    assert _hit(c, q0)
    assert _hit(c, q1, k1)
    # Other topics must not mix in: drop this topic to 1 message and pile 5 onto the other topic
    _stage(c, q1, [])
    _stage(c, other, _n_sized(OTHER_TOPIC, 1, 5))
    assert not _hit(c, q0), "another topic's buffer must not count toward this topic's cumulative total"


def test_topic_size_gate_has_its_own_switch_and_runs_last():
    c = _consumer("G_fc_topic_size")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    _gates_off(c)
    # The queue-level byte gate is **off** (0), leaving only the topic level: misusing the queue-level switch as the gate would silently disable this
    c.pull_threshold_size_for_topic = 1
    _stage(c, q0, [_sized(TOPIC, MIB // 2, 0)])
    assert not _hit(c, q0)
    _stage(c, q1, [_sized(TOPIC, 3 * MIB // 2, 0)])
    assert _hit(c, q0), "when the queue-level size gate is off, the topic-level size gate must still take effect"
    assert _hit(c, q1)
    # While the queue-level gate is still on, the queue level hits first (judgment order: count → bytes → span → topic count → topic bytes)
    c.pull_threshold_size_for_queue = 1
    _stage(c, q0, [_sized(TOPIC, 2 * MIB, 0)])
    before = c._flow_control_triggered
    assert _hit(c, q0)
    # One judgment records only one tick, even when multiple gates hit simultaneously
    assert c._flow_control_triggered == before + 1


def test_one_hit_counts_one_tick_even_when_every_gate_matches():
    """The counter is an observability surface for "how many times flow control stopped things": even when all five gates hit at once, only one tick is added."""
    c = _consumer("G_fc_counter")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    c.pull_threshold_size_for_queue = 1
    c.consume_concurrently_max_span = 1
    c.pull_threshold_for_topic = 1
    c.pull_threshold_size_for_topic = 1
    # A buffer of 2 messages at 2MiB each with offsets spanning to 100: all five gates hit at once
    _stage(c, q0, [_sized(TOPIC, 2 * MIB, 0), _sized(TOPIC, 2 * MIB, 100)])
    _stage(c, q1, [_sized(TOPIC, 2 * MIB, 0)])
    assert _hit(c, q0)
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
