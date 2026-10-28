# Why this must be locked offline: of the three per-queue gates, only the **count** one fires first in "small messages, fast pulls" scenarios;
# the other two fail in a **silent** direction when judged wrong:
#   - Queue-level byte gate failing ⇒ large messages blow up the held pile (real-world symptom: OOM / RT spike, no exception);
#   - Offset span gate failing ⇒ the head message keeps failing to consume while the rest piles up endlessly, and the offset span runs away;
#   - Topic-level division failing ⇒ the topic budget is never turned into a per-queue quota, so the instance total explodes while no single queue ever looks "over".
# The units have exactly one pitfall: the size threshold is **whole MiB of body bytes**, not bytes and not the
# on-disk record length. The comparison has one, too: every cache gate is **strictly greater than** the
# threshold with no clamp and no "0 disables" escape, so a queue that has reached it exactly still pulls
# while a threshold of 0 is refused by the first held message.
# What is measured is everything the queue still holds: the pulled-but-undispatched buffer **plus** the
# batches already handed to the listener and not settled yet. A gate that read only the buffer would let a
# wedged listener pull without bound.
#
# This file locks the offline semantics of flow-control judgment: the hit direction and units of the three per-queue
# gates, how the two topic knobs are divided into them when the queue assignment changes,
# plus the two stalls the gate can cause (the client's own cache threshold vs the broker's FLOW_CONTROL(215) reply).
# The POP path's cache gate (this queue's wait-ack debt against ``pop_threshold_for_queue``) is locked here too:
# it compares the same way, judges only its own queue, and counts on the **same** flow-control counter as the pull
# path, so one client-wide total and one warn cadence span both paths.
# The LitePull path gets its own section at the bottom: four budgets over one shared local buffer (three per queue
# plus an aggregate that budgets every pull round still waiting to be polled), per-queue accumulators that must stay
# in step with that buffer, and a single round loop that walks every assigned queue -- so a parked queue must never
# delay its siblings.
# For the real-machine side, "no message is lost after a gate hits", see ../verify_flow_control_live.py.
import inspect
import threading
import time

from client import consumer as consumer_mod
from client.consumer import (DefaultLitePullConsumer, DefaultMQPushConsumer, PopProcessQueue,
                             FLOW_CONTROL_WARN_EVERY_N_TRIPS,
                             PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL,
                             PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL,
                             is_flow_control_warn_trip)
from client.consumer_result import (ConsumeConcurrentlyStatus, ConsumeOrderlyStatus,
                                    MessageListenerConcurrently, MessageListenerOrderly,
                                    PullResult, PullStatus)
from client.exception import MQBrokerException
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.codes import ResponseCode
from remoting.protocol.heartbeat import MessageModel
from tests.test_lite_pull_consumer import _Lite

TOPIC = "FlowControlPyUnitTopic"
BROKER = "broker-a"
MIB = 1024 * 1024
# A threshold no test under measurement can reach: with the gates compared plainly, "off" can only mean
# "so large it never trips", never 0.
OUT_OF_REACH = 2 ** 31 - 1


def _queue(topic=TOPIC, queue_id=0):
    return MessageQueue(topic=topic, broker_name=BROKER, queue_id=queue_id)


def _sized(topic, body_size, queue_offset, store_size=None):
    """A message whose **body** is ``body_size`` bytes: the cache size gate measures the body, which is
    what this client actually holds. ``store_size`` is set to something else on purpose in the tests that
    pin the two apart."""
    m = MessageExt(topic=topic, body=b"x" * body_size)
    m.broker_name = BROKER
    m.store_size = store_size if store_size is not None else body_size
    m.queue_offset = queue_offset
    return m


def _n_sized(topic, body_size, n):
    return [_sized(topic, body_size, i) for i in range(n)]


def _consumer(group):
    """A not-yet-started consumer: _pending/_mq_map are pre-set by the test itself, no network touched."""
    return DefaultMQPushConsumer(group)


def _gates_off(c):
    """Turn off all gates except the threshold under test: an earlier hit would mask the branch under test."""
    c.pull_threshold_for_queue = OUT_OF_REACH
    c.pull_threshold_size_for_queue = OUT_OF_REACH
    c.consume_concurrently_max_span = OUT_OF_REACH
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


def test_count_gate_fires_above_threshold_with_no_floor_on_the_threshold():
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
    # Threshold 0 is the user asking for "cache nothing", so the first held message trips it: the gate takes
    # the configured number as it stands, with no max(1, n) floor quietly turning 0 into 1.
    c.pull_threshold_for_queue = 0
    _stage(c, mq, [])
    assert not _hit(c, mq), "an empty queue is not yet over a zero budget"
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert _hit(c, mq), "pull_threshold_for_queue=0 must trip on the first held message"


def test_size_gate_divides_body_bytes_by_whole_mib_with_no_floor_and_no_off_switch():
    c = _consumer("G_fc_size")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = OUT_OF_REACH
    c.pull_threshold_size_for_queue = 1          # 1 MiB
    _stage(c, mq, _n_sized(TOPIC, 300 * 1024, 3))  # 900 KiB of body across 3 messages
    assert not _hit(c, mq)
    # The unit is **whole** MiB by integer division, and the gate is strictly greater: exactly 1 MiB is 1,
    # which is not over a budget of 1.
    _stage(c, mq, [_sized(TOPIC, MIB, 0)])
    assert not _hit(c, mq), "exactly 1MiB must not hit (>, not >=)"
    # 1 MiB + 1 byte of body still divides to 1 MiB: a partial MiB never trips this gate.
    _stage(c, mq, [_sized(TOPIC, MIB + 1, 0)])
    assert not _hit(c, mq), "integer division must floor: 1MiB+1B is still 1 MiB, not over a budget of 1"
    _stage(c, mq, [_sized(TOPIC, 2 * MIB, 0)])
    assert _hit(c, mq), "2 MiB of body exceeds the 1MiB threshold"
    # A huge on-disk record with a small body is not a large backlog for this client: the gate counts the
    # body, which is the bytes it actually holds.
    c._flow_control_triggered = 0
    _stage(c, mq, [_sized(TOPIC, 1024, 0, store_size=8 * MIB)])
    assert not _hit(c, mq), "storeSize is not what this client caches; the size gate must ignore it"
    assert c._flow_control_triggered == 0
    # 0 is not an off switch: it asks for no cached bytes, and the first whole MiB trips it.
    c.pull_threshold_size_for_queue = 0
    _stage(c, mq, [_sized(TOPIC, 2 * MIB, 0)])
    assert _hit(c, mq), "pull_threshold_size_for_queue=0 must trip once the cache divides to 2 MiB"
    _stage(c, mq, [_sized(TOPIC, 1024, 0)])
    assert not _hit(c, mq), "1 KiB divides to 0 MiB, which is not over a zero budget"


def test_span_gate_is_strictly_greater_with_no_off_switch():
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
    # 0 does not switch the gate off: one message spans 0 and still pulls, two spanned messages do not.
    c.consume_concurrently_max_span = 0
    _stage(c, mq, [_sized(TOPIC, 1, 5)])
    assert not _hit(c, mq), "a single held message spans 0, which is not over a zero budget"
    _stage(c, mq, [_sized(TOPIC, 1, 5), _sized(TOPIC, 1, 6)])
    assert _hit(c, mq), "a span of 1 is over a zero budget; 0 means 'do not run ahead', not 'off'"


# ---------------- The gates judge everything the queue still holds, buffer plus listener ----------------
# A batch leaves the pull buffer the moment the dispatch thread takes it, but the queue has not finished with
# it: the message is still cached, it is only being handed to the listener. Measuring the buffer alone lets a
# wedged listener keep pulling -- the messages it holds have left the buffer, so the queue reads empty while
# the application makes no progress. So count, size and span are judged over
# **buffer + batches still in the listener's hands** together.
# The span has one exception, and it is not a narrower scope: ordered consumption does not judge a span at
# all, because its pull cursor cannot run ahead while a batch sits unsettled in the listener.
# Every case below is built so the two answers differ: the buffer alone sits exactly on the budget, and the
# batch on top of it is what crosses the line.

class _ConcurrentGateProbe(MessageListenerConcurrently):
    """Asks the cache gate while it holds the batch, records the answer, then acks the batch."""

    def __init__(self, c, mq, key):
        self._c, self._mq, self._key, self.answers = c, mq, key, []

    def consume_message(self, msgs, context):
        self.answers.append(self._c._flow_control_hit(self._mq, self._key))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


class _OrderlyGateProbe(MessageListenerOrderly):
    """The same probe on the ordered path, where a settled batch commits and leaves the queue."""

    def __init__(self, c, mq, key):
        self._c, self._mq, self._key, self.answers = c, mq, key, []

    def consume_message(self, msgs, context):
        self.answers.append(self._c._flow_control_hit(self._mq, self._key))
        return ConsumeOrderlyStatus.SUCCESS


def _dispatch_head(c, key, mq, probe, n=1):
    """Hand the first ``n`` buffered messages to ``probe`` through the real consume entry point."""
    batch = [c._pending[key].pop(0) for _ in range(n)]
    c._consume_batch(key, mq, batch)
    return batch


def test_concurrent_listener_held_messages_count_toward_the_count_gate():
    c = _consumer("G_fc_inflight_count")
    mq = _queue()
    key = _stage(c, mq, _n_sized(TOPIC, 100, 4))
    _gates_off(c)
    c.pull_threshold_for_queue = 3
    probe = _ConcurrentGateProbe(c, mq, key)
    c.message_listener = probe
    _dispatch_head(c, key, mq, probe)
    assert probe.answers == [True], \
        "3 buffered + 1 in the listener is 4 held, over a budget of 3; reading the buffer alone answers False"
    # The batch settled (acked), so the queue is back to exactly its budget and pulls again: the in-flight
    # term is released with the batch, not left behind.
    assert not _hit(c, mq)
    assert c._inflight_msgs.get(key) is None, "a settled batch must leave the held set"


def test_concurrent_listener_held_messages_count_toward_the_size_gate():
    c = _consumer("G_fc_inflight_size")
    mq = _queue()
    key = _stage(c, mq, [_sized(TOPIC, MIB, off) for off in range(4)])
    _gates_off(c)
    c.pull_threshold_size_for_queue = 3
    probe = _ConcurrentGateProbe(c, mq, key)
    c.message_listener = probe
    _dispatch_head(c, key, mq, probe)
    assert probe.answers == [True], \
        "4 MiB held (3 buffered + 1 in the listener) is over a 3 MiB budget; the buffer alone is exactly 3"
    assert not _hit(c, mq)


def test_concurrent_listener_held_messages_widen_the_span():
    c = _consumer("G_fc_inflight_span")
    mq = _queue()
    key = _stage(c, mq, [_sized(TOPIC, 100, off) for off in (0, 10)])
    _gates_off(c)
    c.consume_concurrently_max_span = 5
    probe = _ConcurrentGateProbe(c, mq, key)
    c.message_listener = probe
    _dispatch_head(c, key, mq, probe)
    assert probe.answers == [True], \
        "concurrent consumption keeps the batch attached to its offsets: the held span is still 10, over 5"
    assert not _hit(c, mq)


def test_ordered_listener_held_messages_count_toward_the_count_and_size_gates():
    c = _consumer("G_fc_orderly_inflight")
    mq = _queue()
    key = _stage(c, mq, [_sized(TOPIC, MIB, off) for off in range(4)])
    _gates_off(c)
    c.pull_threshold_for_queue = 3
    c.pull_threshold_size_for_queue = 3
    probe = _OrderlyGateProbe(c, mq, key)
    c.message_listener = probe
    assert c._is_orderly(), "ordered consumption is chosen by the listener's type"
    _dispatch_head(c, key, mq, probe)
    assert probe.answers == [True], \
        "ordered consumption holds its batch on the queue too: 4 held over a budget of 3 must refuse"
    assert not _hit(c, mq)


def test_ordered_consumption_never_judges_the_span_gate():
    c = _consumer("G_fc_orderly_span")
    mq = _queue()
    # One queue's cache spans 0..20 against a max span of 15, and nothing else is over budget.
    key = _stage(c, mq, [_sized(TOPIC, 1, off) for off in (0, 10, 20)])
    _gates_off(c)
    c.consume_concurrently_max_span = 15
    c.message_listener = _ConcurrentGateProbe(c, mq, key)
    assert _hit(c, mq), \
        "the same cache on the concurrent path trips the span: 20 is over a budget of 15"
    c._flow_control_triggered = 0
    probe = _OrderlyGateProbe(c, mq, key)
    c.message_listener = probe
    assert c._is_orderly(), "ordered consumption is chosen by the listener's type"
    # Ordered consumption is the span gate's one outright exception: it never runs, whatever the span is.
    # A concurrent read of this cache answers True, so only the consume mode separates the two answers.
    assert not _hit(c, mq), \
        "ordered consumption skips the span term entirely -- it does not narrow its scope"
    assert c._flow_control_triggered == 0, "no term was judged while the queue spanned over its budget"
    # The exception is the span alone: count and size still bind an ordered queue, and they read the
    # full held set, dispatched batch included.
    c.pull_threshold_for_queue = 2
    assert _hit(c, mq), "3 held still trips a count budget of 2 on the ordered path"


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
# generic backoff -- which is the consumer's own knob ``pull_time_delay_when_exception_millis``
# (default 1000 ms), not a value the loop invents. Folding both into one delay either hammers a
# throttled broker or needlessly stalls a healthy queue behind a far longer wait.

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


def test_broker_flow_control_pull_retries_in_20ms_other_codes_take_the_error_backoff(monkeypatch):
    assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
    c, mq, key = _error_loop_consumer("G_fc_broker_pull")
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy"),
                          MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")],
                         c, mq, key)
    # 215 gets the short branch, any other broker code takes the generic backoff (the knob's
    # 1000 ms default): the two directions pin each other (reverting either branch turns this red).
    assert delays == [0.02, 1.0], delays


def test_broker_flow_control_pop_retries_in_20ms_other_codes_take_the_error_backoff(monkeypatch):
    pq = PopProcessQueue()

    def _setup():
        c._pop_queues[key] = pq

    c, mq, key = _error_loop_consumer("G_fc_broker_pop")
    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message",
                         [MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy"),
                          MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")],
                         c, mq, key, setup=_setup)
    assert delays == [0.02, 1.0], delays


def test_plain_client_error_pull_loop_keeps_generic_backoff(monkeypatch):
    # Control without a broker code at all: a generic exception must not pick the 20ms branch.
    c, mq, key = _error_loop_consumer("G_fc_generic")
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [RuntimeError("socket reset"),
                          MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy")],
                         c, mq, key)
    assert delays == [1.0, 0.02], delays


# ---------------- the generic error backoff is a per-client knob ----------------
# Four legs, because the knob is the only arm of the backoff table an application may retune: the
# default it ships with, the broker's 215 answer (which must ignore it and stay at 20 ms), the value
# a reconfigured knob puts on the wait, and the non-215 error taking exactly that value -- on both
# the pull and the POP path.

def test_generic_error_backoff_knob_pins_default_215_and_the_configured_value(monkeypatch):
    # Leg 1: the default is 1000 ms, and that is what a generic pull error sleeps.
    c, mq, key = _error_loop_consumer("G_fc_knob_default")
    assert c.get_pull_time_delay_when_exception_millis() == 1000
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [RuntimeError("socket reset")], c, mq, key)
    assert delays == [1.0], delays
    # Leg 2: the broker's 215 answer keeps its own 20 ms however the knob is set -- the knob is
    # never on that branch.
    c2, mq2, key2 = _error_loop_consumer("G_fc_knob_broker_215")
    c2.set_pull_time_delay_when_exception_millis(12345)
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [MQBrokerException(ResponseCode.FLOW_CONTROL, "broker throttling")],
                         c2, mq2, key2)
    assert delays == [PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL], delays
    # Leg 3: a non-215 broker code sleeps the value the knob was set to, not the default.
    c3, mq3, key3 = _error_loop_consumer("G_fc_knob_pull_set")
    c3.set_pull_time_delay_when_exception_millis(250)
    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")], c3, mq3, key3)
    assert delays == [0.25], delays
    # Leg 4: the POP path reads the same knob on its non-215 errors.
    c4, mq4, key4 = _error_loop_consumer("G_fc_knob_pop_set")
    c4.set_pull_time_delay_when_exception_millis(250)
    pq = PopProcessQueue()

    def _setup():
        c4._pop_queues[key4] = pq

    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message",
                         [RuntimeError("socket reset")], c4, mq4, key4, setup=_setup)
    assert delays == [0.25], delays
    # A negative value is read as 0 ("pull again at once"), never a sleep into the past.
    c4.set_pull_time_delay_when_exception_millis(-1)
    assert c4.get_pull_time_delay_when_exception_millis() == 0


def test_error_backoff_knob_is_taken_when_the_loop_is_set_up(monkeypatch):
    """The loop takes the configured value when it is set up: retuning the knob while a queue loop
    is running does not change the wait that loop takes -- the next loop this consumer creates does."""
    c, mq, key = _error_loop_consumer("G_fc_knob_snapshot")
    c.set_pull_time_delay_when_exception_millis(250)
    real_time = time
    seen = []

    class _Client(object):
        def pull_message(self, *a, **k):
            raise RuntimeError("socket reset")

    class _FakeTime(object):
        def __getattr__(self, name):
            return getattr(real_time, name)

        def sleep(self, seconds):
            seen.append(seconds)
            # Reconfigure the knob from inside a wait this loop already committed to.
            c.pull_time_delay_when_exception_millis = 9000
            if len(seen) == 2:
                c._stop.set()

    c._mq_client = _Client()
    monkeypatch.setattr(consumer_mod, "time", _FakeTime())
    th = threading.Thread(target=c._queue_pull_loop, args=(mq,))
    c._queue_threads[key] = th
    th.start()
    deadline = real_time.time() + 10
    while th.is_alive() and real_time.time() < deadline:
        real_time.sleep(0.01)
    c._stop.set()
    th.join(timeout=5)
    assert seen == [0.25, 0.25], \
        "both rounds ran on the value the loop was set up with, not on the one set mid-loop"


# ---------------- The client's own cache threshold vs the broker's 215 reply ----------------
# Both stall the pull, for opposite reasons: the local buffer is full (wait for the listener to
# drain it, 50ms) vs the broker is throttling (short retry, 20ms). Folding them into one delay, or
# leaving the local one longer than the drain interval, either stalls a healthy queue or spins.

def test_cache_flow_control_backoff_is_50ms_and_distinct_from_broker_215():
    assert PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL == 0.05
    assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
    # The generic error wait is the consumer's knob (default 1000 ms), not one of these constants,
    # and it stays a third distinct number: an application that retunes it must not move either
    # flow-control stall.
    assert _consumer("G_fc_knob_defaults").get_pull_time_delay_when_exception_millis() == 1000
    # The pull-loop stall must actually use the constant, not a literal of its own.
    src = inspect.getsource(DefaultMQPushConsumer._queue_pull_loop)
    assert "PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL" in src
    assert "time.sleep(0.1)" not in src
    # ... and the loop's generic error arm must read the knob, not a literal of its own either.
    assert "pull_time_delay_when_exception_millis" in src
    assert "time.sleep(0.5)" not in src


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
    # broker and records the *error* backoff instead of the cache one. The knob is set to a value
    # no other arm of the table uses, so what is slept can only have come from the knob.
    c, mq, key = _error_loop_consumer("G_fc_cache_pass")

    def _setup():
        _gates_off(c)
        c.pull_threshold_for_queue = 1
        c.set_pull_time_delay_when_exception_millis(777)
        _stage(c, mq, _n_sized(TOPIC, 100, 1))   # 1 message at a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pull_loop", "pull_message",
                         [RuntimeError("socket reset")], c, mq, key, setup=_setup)
    assert delays == [0.777], delays
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
    # reaches the broker and records the *error* backoff instead of the cache one. The knob is set
    # to a value no other arm uses, so the slept number can only have come from the knob.
    c, mq, key = _error_loop_consumer("G_fc_pop_cache_pass")

    def _setup():
        c.pop_threshold_for_queue = 1
        c.set_pull_time_delay_when_exception_millis(777)
        c._pop_queues[key] = _pop_debt(1)      # 1 message at a threshold of 1

    delays = _drive_loop(monkeypatch, "_queue_pop_loop", "pop_message",
                         [RuntimeError("socket reset")], c, mq, key, setup=_setup)
    assert delays == [0.777], delays
    assert c._flow_control_triggered == 0


# ================================ The LitePull path: four budgets over one shared local buffer ================================
# The LitePull consumer runs a background pull that feeds **one** global buffer drained only by poll(), so an
# application that polls slowly -- or never -- grows that buffer without bound. Four gates refuse the pull before
# it is issued (an aggregate over every pull round still waiting to be polled, plus this queue's count / MiB /
# offset span) and re-schedule **that queue's** next round 50 ms later. What makes this path its own case, rather
# than a copy of the push gates:
#   * the per-queue verdict comes from incremental accumulators that must stay in step with a shared container
#     through append, poll, seek and queue revocation;
#   * the round loop is a single thread walking every assigned queue, so parking one queue must not delay a
#     sibling, and a refusal must keep the request away from the broker entirely;
#   * all four budgets are plain numbers: compared strictly greater, never clamped, so 0 legally means "trip on
#     the first cached message" rather than "this gate is off".
# The aggregate budgets a **round** (a pending request may still carry a whole batch) rather than the messages
# that round happened to bring back, so a backlog of half-empty rounds is refused earlier than its message count
# suggests. The size gate divides by whole MiB and sums the message **body lengths**, never the on-disk record
# size; the span gate reads only this queue's own cached offsets. Broker FLOW_CONTROL(215) on this path is a
# 20 ms re-schedule, not an error.

LITE_TOPIC = "LiteFlowControlPyUnitTopic"
# One poll() hand-out is capped, which is what leaves a real partial buffer behind for the gates to keep
# refusing: the number is the cap in poll(), and the case below pins it so the two cannot drift apart.
POLL_BATCH_LIMIT = 1024


def _lite_mq(queue_id=0, topic=LITE_TOPIC):
    return MessageQueue(topic=topic, broker_name=BROKER, queue_id=queue_id)


def _lite_msg(body_size, queue_offset, store_size=1, queue_id=0, topic=LITE_TOPIC):
    """A message as the lite buffer sees it: the body length and the queueOffset the accumulators read, and a
    record size chosen **independently** of both, so the two can never be confused by the size gate."""
    m = MessageExt(topic=topic, body=b"y" * body_size)
    m.broker_name = BROKER
    m.queue_id = queue_id
    m.queue_offset = queue_offset
    m.store_size = store_size
    return m


class _BoundsClient:
    """The two boundary reads Seek makes. The buffer under test is local, so the range
    only has to be wide enough for the offsets those tests pin."""

    def get_min_offset(self, mq):
        return 0

    def get_max_offset(self, mq):
        return 10 ** 9


def _lite_consumer(group, *mqs):
    """A running-but-threadless LitePull consumer: no thread and no socket, and the gates read exactly
    what the shared local buffer holds. ``assign()`` is what makes the queues owned, which is what the gates
    judge -- the same path a live consumer takes. The started flag and the boundary client are set by hand
    because ``poll()`` and ``seek()`` refuse a consumer that never started, and these tests hand the buffer
    to the application.
    """
    c = DefaultLitePullConsumer(group)
    c.set_auto_commit(False)
    if mqs:
        c.assign(list(mqs))
    c._started = True
    c._mq_client = _BoundsClient()
    return c


def _lite_gates_off(c):
    """Turn every budget but the one under test out of reach: an earlier gate would mask that branch."""
    c.pull_threshold_for_all = OUT_OF_REACH
    c.pull_threshold_for_queue = OUT_OF_REACH
    c.pull_threshold_size_for_queue = OUT_OF_REACH
    c.consume_max_span = OUT_OF_REACH


def _lite_cache(c, msgs):
    """Fill the local buffer through the real append path, so the accumulators under test are the ones the
    production pull uses -- not a test-only fixture the buffer could drift away from."""
    c._enqueue(msgs)
    return c


def _lite_with_backlog(group, msgs, queue_id=0):
    """A consumer owning one queue that already caches ``msgs``."""
    mq = _lite_mq(queue_id)
    c = _lite_consumer(group, mq)
    _lite_gates_off(c)
    _lite_cache(c, msgs)
    return c, mq


def _lite_hit(c, mq):
    return c._cache_flow_control_hit(mq)


def _lite_warns(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelname == "WARNING" and "flow control" in r.getMessage()]


def _lite_debugs(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelname == "DEBUG" and "flow control" in r.getMessage()]


class _ScriptedLiteClient(object):
    """Only what the lite round loop calls, with a record of every request and two ways to answer badly.

    A script entry is either a PullResult or an exception raised "from inside the request"; once the script is
    exhausted the queue tail answers NO_NEW_MSG, which changes nothing but the round's idle backoff. The
    round walks the assigned queues in no particular order, so a case that needs one named queue to fail names
    it in ``error_on_queue`` instead of relying on the script position.
    """

    def __init__(self, answers=(), error_on_queue=None):
        self._answers = list(answers)
        self._error_queue_id, self._error = error_on_queue if error_on_queue else (None, None)
        self.requests = []

    def pull_message(self, consumer_group, mq, queue_offset, max_msg_nums, sys_flag,
                     commit_offset, subscription, sub_version, expression_type,
                     timeout_millis=30000, max_msg_bytes=-1, suspend_timeout_millis=15000,
                     addr=None, request_source=0):
        self.requests.append((mq.queue_id, queue_offset))
        if self._error is not None and mq.queue_id == self._error_queue_id:
            error = self._error
            self._error = None
            raise error
        if not self._answers:
            return PullResult(PullStatus.NO_NEW_MSG, queue_offset, 0, 0, [])
        answer = self._answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def _lite_round_consumer(group, mqs, answers=(), error_on_queue=None):
    """A LitePull consumer the **real** round loop drives.

    Assign mode (so the round does not rebalance) with the pull cursors pinned: nothing resolves an offset over
    the wire except the scripted pulls, and the idle backoff is 500 ms so it can never be mistaken for the
    50 ms cache pause.
    """
    c = _lite_consumer(group, *mqs)
    for mq in mqs:
        c._next_offset[mq] = 0
    c.set_pull_interval_millis(500)
    client = _ScriptedLiteClient(answers, error_on_queue)
    c._mq_client = client
    return c, client


def _drive_lite_rounds(monkeypatch, c, rounds, on_round_end=None):
    """Run ``_pull_service_loop`` for ``rounds`` rounds on a virtual clock and return the recorded pauses.

    Every pause the loop takes is recorded **and paid** by advancing the virtual clock, so these cases assert
    on the scheduled delay instead of sleeping: the loop stops once the requested number of rounds is done,
    never on a wall-clock window.
    """
    sleeps = []
    real_time = time

    class _VirtualTime(object):
        # The clock starts at exactly zero: every scheduled pause is then a difference of two small doubles, so
        # the recorded sleeps compare to the constants bit for bit instead of drifting on a large base.
        def __init__(self):
            self.now = 0.0

        def __getattr__(self, name):
            return getattr(real_time, name)

        def time(self):
            return self.now

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            sleeps.append(seconds)
            self.now += seconds
            if len(sleeps) >= rounds:
                c._running = False
            elif on_round_end is not None:
                on_round_end()

    c._running = True
    monkeypatch.setattr(consumer_mod, "time", _VirtualTime())
    try:
        c._pull_service_loop()
    finally:
        c._running = False
    return sleeps


# ---------------- the four knobs, their setters/getters and the strict boundaries ----------------

def test_lite_knob_defaults_are_the_four_budgets():
    c = DefaultLitePullConsumer("G_lite_knob_defaults")
    assert c.pull_threshold_for_all == 10000
    assert c.pull_threshold_for_queue == 1000
    assert c.pull_threshold_size_for_queue == 100
    assert c.consume_max_span == 2000
    # Nothing cached at all is under every budget: the gates must not refuse the first pull of a queue.
    mq = _lite_mq()
    c.assign([mq])
    assert not _lite_hit(c, mq)
    assert (c._flow_control_for_all_triggered, c._flow_control_triggered,
            c._flow_control_span_triggered) == (0, 0, 0)


def test_lite_knob_setters_and_getters_round_trip_without_clamping():
    c = DefaultLitePullConsumer("G_lite_knob_setters")
    c.set_pull_threshold_for_all(500)
    c.set_pull_threshold_for_queue(0)
    c.set_pull_threshold_size_for_queue(3)
    c.set_consume_max_span(0)
    assert c.get_pull_threshold_for_all() == 500
    assert c.get_pull_threshold_for_queue() == 0
    assert c.get_pull_threshold_size_for_queue() == 3
    assert c.get_consume_max_span() == 0
    assert (c.pull_threshold_for_queue, c.consume_max_span) == (0, 0), \
        "a zero budget is a legal configuration, so the setters must not clamp it to 1"


def test_lite_count_gate_is_strictly_greater_and_zero_trips_on_the_first_message():
    c, mq = _lite_with_backlog("G_lite_count", [_lite_msg(100, i) for i in range(2)])
    c.pull_threshold_for_queue = 3
    assert not _lite_hit(c, mq)
    # Exactly AT the threshold still pulls (>, not >=).
    _lite_cache(c, [_lite_msg(100, 2)])
    assert not _lite_hit(c, mq), "3 cached at a threshold of 3 must not hit (>, not >=)"
    # One more message and this round is refused.
    _lite_cache(c, [_lite_msg(100, 3)])
    assert _lite_hit(c, mq)
    assert c._flow_control_triggered == 1
    # 0 is "trip on the first cached message", not "gate off" -- the paired control is the empty cache
    # above, which a zero budget must still let pull.
    zero, zmq = _lite_with_backlog("G_lite_count_zero", [_lite_msg(100, 0)])
    zero.pull_threshold_for_queue = 0
    assert _lite_hit(zero, zmq), "pull_threshold_for_queue=0 must trip on the first cached message"
    empty = _lite_consumer("G_lite_count_zero_control", zmq)
    empty.pull_threshold_for_queue = 0
    assert not _lite_hit(empty, zmq), "an empty cache is not yet over a zero budget"


def test_lite_size_gate_measures_the_body_not_the_stored_record():
    # A 2 MiB body carried in a 1 KiB store record: what this client holds is 2 MiB of bytes, so the gate
    # refuses. A storeSize-based implementation would read ~0 MiB here and let the backlog grow forever.
    big, big_mq = _lite_with_backlog("G_lite_size_body",
                                    [_lite_msg(2 * MIB, 0, store_size=1024)])
    big.pull_threshold_size_for_queue = 1
    assert _lite_hit(big, big_mq), "the size gate must count the body length, not storeSize"


def test_lite_size_gate_ignores_a_large_store_record_with_a_small_body():
    # Paired control: two 2 MiB store records carrying a 1 KiB body each. Counted by storeSize that is 4 MiB over
    # a 1 MiB threshold and the queue would be refused; counted by body length it is 2 KiB, so it keeps pulling.
    small, small_mq = _lite_with_backlog("G_lite_size_body_control",
                                         [_lite_msg(1024, i, store_size=2 * MIB) for i in range(2)])
    small.pull_threshold_size_for_queue = 1
    assert not _lite_hit(small, small_mq)
    assert small._flow_control_triggered == 0


def test_lite_size_gate_divides_by_whole_mib_with_integer_division():
    for body_size, expected in ((MIB - 1, False), (MIB, False),
                                (MIB + 1, False), (2 * MIB, True), (2 * MIB + 1, True)):
        c, mq = _lite_with_backlog("G_lite_size_divide", [_lite_msg(body_size, 0)])
        c.pull_threshold_size_for_queue = 1
        assert _lite_hit(c, mq) is expected, "%d bytes must divide to %s a 1 MiB budget" % (
            body_size, "refuse" if expected else "pull")
    # A partially filled MiB never counts as one: 3 x 300 KiB is 900 KiB, still 0 MiB.
    c, mq = _lite_with_backlog("G_lite_size_divide_partial",
                               [_lite_msg(300 * 1024, i) for i in range(3)])
    c.pull_threshold_size_for_queue = 1
    assert not _lite_hit(c, mq)


def test_lite_span_gate_is_strictly_greater_and_measures_this_queue_only():
    at, at_mq = _lite_with_backlog("G_lite_span_at",
                                   [_lite_msg(1, 0), _lite_msg(1, 10)])
    at.consume_max_span = 10
    assert not _lite_hit(at, at_mq), "a span of exactly 10 at a max span of 10 still pulls (>, not >=)"
    over, over_mq = _lite_with_backlog("G_lite_span_over",
                                       [_lite_msg(1, 0), _lite_msg(1, 11)])
    over.consume_max_span = 10
    assert _lite_hit(over, over_mq)
    assert over._flow_control_span_triggered == 1
    assert over._flow_control_triggered == 0, "the span gate has a counter of its own"
    # An out-of-order buffer measures the real span (max - min), not the first-last difference.
    shuffled, shuffled_mq = _lite_with_backlog("G_lite_span_order",
                                               [_lite_msg(1, 50), _lite_msg(1, 5), _lite_msg(1, 7)])
    shuffled.consume_max_span = 10
    assert _lite_hit(shuffled, shuffled_mq), "an out-of-order cache must measure span as max-min"
    # A single cached message has span 0, so a zero budget still lets the first one through.
    single, single_mq = _lite_with_backlog("G_lite_span_single", [_lite_msg(1, 5000)])
    single.consume_max_span = 0
    assert not _lite_hit(single, single_mq), "one cached message spans 0"
    _lite_cache(single, [_lite_msg(1, 5001)])
    assert _lite_hit(single, single_mq), "two cached messages span 1, which is over a zero budget"
    # The span is this queue's: a sibling holding 0..1000 does not widen it.
    hot = _lite_mq(0)
    wild = _lite_mq(1)
    c = _lite_consumer("G_lite_span_sibling", hot, wild)
    _lite_gates_off(c)
    c.consume_max_span = 10
    _lite_cache(c, [_lite_msg(1, 0, queue_id=1), _lite_msg(1, 1000, queue_id=1),
                    _lite_msg(1, 4, queue_id=0), _lite_msg(1, 7, queue_id=0)])
    assert not _lite_hit(c, hot), "a sibling queue's offset window must not stop this queue"
    assert _lite_hit(c, wild)


def test_lite_aggregate_gate_budgets_every_pending_round_not_only_the_messages_that_arrived():
    queues = [_lite_mq(i) for i in range(4)]
    c = _lite_consumer("G_lite_all", *queues)
    _lite_gates_off(c)
    c.pull_batch_size = 32
    c.pull_threshold_for_all = 100
    # Four rounds that came back half empty: 8 messages sit in the shared buffer, and no queue is anywhere
    # near its own count budget -- but each pending round can still carry a whole batch of 32, so the
    # capacity already reserved for poll() is 128. That is over a budget of 100, so every queue is refused.
    _lite_cache(c, [_lite_msg(1, j, queue_id=i) for i in range(4) for j in range(2)])
    assert len(c._local_buffer) == 8, "the buffer itself is well under the aggregate budget"
    for mq in queues:
        assert _lite_hit(c, mq), "the aggregate budget is instance-wide: it refuses every queue"
    assert c._flow_control_for_all_triggered == 4
    assert c._flow_control_triggered == 0, "the aggregate gate has a counter of its own"
    assert sum(s.pending_rounds for s in c._queue_cache.values()) == 4


def test_lite_aggregate_budget_is_separate_from_the_per_queue_budgets():
    queues = [_lite_mq(i) for i in range(4)]
    # Paired control on the same buffers: raise the aggregate budget to exactly the reserved capacity and
    # nothing is refused, which is what makes the aggregate a budget of its own rather than a re-reading of
    # the per-queue ones. And it is strictly greater, so capacity sitting ON the budget still pulls.
    c = _lite_consumer("G_lite_all_control", *queues)
    _lite_gates_off(c)
    c.pull_batch_size = 32
    c.pull_threshold_for_all = 128
    _lite_cache(c, [_lite_msg(1, j, queue_id=i) for i in range(4) for j in range(2)])
    for mq in queues:
        assert not _lite_hit(c, mq), "4 rounds x 32 at an aggregate budget of 128 still pulls (>, not >=)"
    assert c._flow_control_for_all_triggered == 0
    # One queue over its own count budget while the reserved capacity stays under the aggregate: only that
    # queue is refused, and on the per-queue counter.
    c2 = _lite_consumer("G_lite_all_split", *queues)
    _lite_gates_off(c2)
    c2.pull_batch_size = 32
    c2.pull_threshold_for_all = 128
    c2.pull_threshold_for_queue = 1
    _lite_cache(c2, [_lite_msg(1, j, queue_id=0) for j in range(3)])
    assert _lite_hit(c2, queues[0]), "3 cached over a per-queue budget of 1 must refuse that queue"
    for mq in queues[1:]:
        assert not _lite_hit(c2, mq)
    assert c2._flow_control_for_all_triggered == 0
    assert c2._flow_control_triggered == 1


def test_lite_aggregate_budget_is_released_round_by_round_as_the_application_polls():
    """A round keeps its whole batch reserved until its last message leaves the buffer: a partially polled
    round still refuses the pull, and only a fully drained one frees its share of the budget."""
    mq = _lite_mq()
    c = _lite_consumer("G_lite_all_rounds", mq)
    _lite_gates_off(c)
    c.pull_batch_size = 4
    c.pull_threshold_for_all = 4
    _lite_cache(c, [_lite_msg(1, 0)])          # round 1: 1 message, reserves a batch of 4
    _lite_cache(c, [_lite_msg(1, 1)])          # round 2: same -> 8 reserved, one over the budget of 4
    assert c._queue_cache[mq].pending_rounds == 2
    assert _lite_hit(c, mq), "2 pending rounds reserve 8 bytes of budget over a threshold of 4"
    # Two messages cached, which is nothing on its own: the budget is the rounds, not the count.
    assert c._queue_cache[mq].count == 2
    drained = c.poll(timeout=0)
    assert [m.queue_offset for m in drained] == [0, 1]
    assert c._queue_cache[mq].pending_rounds == 0
    assert not _lite_hit(c, mq), "polled rounds must stop reserving capacity"
    # And a single round of the very same size stays inside the same budget.
    one = _lite_consumer("G_lite_all_rounds_control", mq)
    _lite_gates_off(one)
    one.pull_batch_size = 4
    one.pull_threshold_for_all = 4
    _lite_cache(one, [_lite_msg(1, 0), _lite_msg(1, 1)])   # one round of 2 messages = exactly 4 reserved
    assert one._queue_cache[mq].pending_rounds == 1
    assert not _lite_hit(one, mq), "one round at exactly the aggregate budget still pulls"


def test_lite_gates_are_judged_in_the_contract_order_with_one_trip_per_refusal():
    # The aggregate goes first: a queue over both the aggregate and its own count budget ticks the
    # aggregate counter only.
    mq = _lite_mq()
    c = _lite_consumer("G_lite_order_all", mq)
    c.pull_threshold_for_all = 1
    c.pull_threshold_for_queue = 0
    _lite_cache(c, [_lite_msg(100, 0), _lite_msg(100, 1)])
    assert _lite_hit(c, mq)
    assert c._flow_control_for_all_triggered == 1
    assert c._flow_control_triggered == 0
    # Then the count, then the size, then the span: a queue over all three of the per-queue budgets ticks
    # exactly one counter, and it is the count one.
    c2, mq2 = _lite_with_backlog("G_lite_order_rest",
                                 [_lite_msg(2 * MIB, 0), _lite_msg(2 * MIB, 100)])
    c2.pull_threshold_for_queue = 1
    c2.pull_threshold_size_for_queue = 1
    c2.consume_max_span = 10
    assert _lite_hit(c2, mq2)
    assert (c2._flow_control_triggered, c2._flow_control_span_triggered) == (1, 0)
    # With the count gate lifted the same buffer is judged by the size gate, which shares that counter.
    c3, mq3 = _lite_with_backlog("G_lite_order_size",
                                 [_lite_msg(2 * MIB, 0), _lite_msg(2 * MIB, 100)])
    c3.pull_threshold_size_for_queue = 1
    c3.consume_max_span = 10
    assert _lite_hit(c3, mq3)
    assert (c3._flow_control_triggered, c3._flow_control_span_triggered) == (1, 0)


def test_lite_unassigned_and_revoked_queues_are_never_refused_by_a_stale_accumulator():
    hot = _lite_mq(0)
    other = _lite_mq(1)
    c = _lite_consumer("G_lite_revoke", hot, other)
    _lite_gates_off(c)
    c.pull_threshold_for_queue = 0
    _lite_cache(c, [_lite_msg(100, 0, queue_id=0)])
    assert _lite_hit(c, hot)
    # Reassigning takes the queue away, and its accumulator goes with the assignment it described.
    c.assign([other])
    assert hot not in c._queue_cache, "a revoked queue must not keep cache state behind"
    assert _lite_hit(c, hot) is False, "a stale accumulator of a queue nobody owns must never refuse"
    assert c._cache_flow_control_hit(_lite_mq(7)) is False
    assert c._flow_control_triggered == 1, "only the live trip counts; the stale state adds nothing"


# ---------------- the accumulators must not be able to drift from the shared buffer ----------------

def test_lite_accumulators_stay_in_step_with_the_buffer_through_append_poll_and_seek():
    q0 = _lite_mq(0)
    q1 = _lite_mq(1)
    c = _lite_consumer("G_lite_consistency", q0, q1)
    _lite_gates_off(c)
    _lite_cache(c, [_lite_msg(1000, 0, queue_id=0),
                    _lite_msg(2000, 5, queue_id=1),
                    _lite_msg(3000, 6, queue_id=1)])
    assert (c._queue_cache[q0].count, c._queue_cache[q0].size) == (1, 1000)
    assert (c._queue_cache[q1].count, c._queue_cache[q1].size, c._queue_cache[q1].span) == (2, 5000, 1)
    # One append is one round per touched queue, whichever number of messages it brought back.
    assert (c._queue_cache[q0].pending_rounds, c._queue_cache[q1].pending_rounds) == (1, 1)
    assert sum(s.count for s in c._queue_cache.values()) == len(c._local_buffer)

    # poll() hands messages out; the same step subtracts them, so the gate reads what is left.
    handed = c.poll(timeout=0)
    assert [m.queue_id for m in handed] == [0, 1, 1]
    assert len(c._local_buffer) == 0
    assert sum(s.count for s in c._queue_cache.values()) == 0
    assert sum(s.pending_rounds for s in c._queue_cache.values()) == 0, \
        "a polled-away round must stop reserving its batch"
    assert (c._queue_cache[q1].size, c._queue_cache[q1].span) == (0, 0), \
        "an emptied queue must read as nothing cached, not as the boundaries of the drained messages"

    # A partial drain leaves exactly the remainder counted.
    _lite_cache(c, [_lite_msg(1000, off, queue_id=0) for off in (0, 4, 9)])
    assert c._queue_cache[q0].span == 9
    c._local_buffer.appendleft(_lite_msg(7, 99, queue_id=1))   # one message of the sibling ahead of them
    drained = c.poll(timeout=0)
    assert len(drained) == 4
    assert sum(s.count for s in c._queue_cache.values()) == len(c._local_buffer) == 0
    assert sum(s.pending_rounds for s in c._queue_cache.values()) == 0

    # seek() prunes the buffer, so it prunes the counters with it.
    _lite_cache(c, [_lite_msg(1000, off, queue_id=0) for off in (0, 4, 9)])
    c.seek(q0, 5)
    assert c._queue_cache[q0].count == 1
    assert (c._queue_cache[q0].min_offset, c._queue_cache[q0].max_offset) == (9, 9)
    assert c._queue_cache[q0].size == 1000
    # The pruned prefix leaves the round it belonged to: one round still holds the message that survived.
    assert c._queue_cache[q0].pending_rounds == 1
    assert sum(s.count for s in c._queue_cache.values()) == len(c._local_buffer) == 1


def test_lite_gates_refuse_a_partially_polled_buffer_and_recover_once_it_is_dry():
    # poll() hands the application at most POLL_BATCH_LIMIT messages, so a slow consumer leaves a real partial
    # buffer behind: the gate must follow what is still cached, round after round, and stop refusing the moment
    # the buffer is dry -- it stalls a slow application, it never deadlocks the consumer.
    mq = _lite_mq()
    c = _lite_consumer("G_lite_drain_recovers", mq)
    _lite_gates_off(c)
    c.pull_threshold_for_queue = 2
    backlog = POLL_BATCH_LIMIT + 76
    _lite_cache(c, [_lite_msg(100, i) for i in range(backlog)])
    assert _lite_hit(c, mq)
    first = c.poll(timeout=0)
    assert len(first) == POLL_BATCH_LIMIT, "poll caps one hand-out, the rest stays cached"
    assert len(c._local_buffer) == backlog - POLL_BATCH_LIMIT
    assert c._queue_cache[mq].count == len(c._local_buffer)
    assert _lite_hit(c, mq), "a partially drained queue is still over its budget, so it stays refused"
    while c._local_buffer:                  # the application catches up: every hand-out is subtracted
        assert _lite_hit(c, mq) is (c._queue_cache[mq].count > 2)
        c.poll(timeout=0)
    assert not _lite_hit(c, mq), "a drained buffer must let the queue pull again"


# ---------------- the refusal at the top of the round: no request, and the 50 ms re-schedule ----------------

def test_lite_cache_refusal_sends_no_request_and_pauses_that_queue_50ms(monkeypatch):
    assert PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL == 0.05
    mq = _lite_mq()
    c, client = _lite_round_consumer("G_lite_round_refuse", [mq])
    c.pull_threshold_for_queue = 1
    _lite_cache(c, [_lite_msg(100, 0), _lite_msg(100, 1)])      # 2 cached over a budget of 1
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=1)
    assert sleeps == [PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL], sleeps
    assert client.requests == [], "a refused round must not let a pull reach the broker"
    assert c._flow_control_triggered == 1
    # the pause is this queue's own re-schedule, 50 ms after the round that refused it (the virtual clock
    # started at zero, so the deadline is the delay itself)
    assert c._pull_not_before == {mq: PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL}
    # and nothing else moved: no offset committed, the pull cursor untouched, the messages still cached
    assert c.pull_cursor_of(mq) == 0
    assert c.consume_cursor_of(mq) == -1
    assert c.pending_commit_of(mq) == -1
    assert len(c._local_buffer) == 2


def test_lite_cache_at_the_threshold_reaches_the_broker(monkeypatch):
    # Paired control: a cache sitting exactly ON the budget pulls, so the round reaches the scripted broker
    # and takes the plain idle backoff instead of the cache pause.
    mq = _lite_mq()
    c, client = _lite_round_consumer("G_lite_round_pass", [mq])
    c.pull_threshold_for_queue = 2
    _lite_cache(c, [_lite_msg(100, 0), _lite_msg(100, 1)])
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=1)
    assert sleeps == [0.5], sleeps
    assert client.requests == [(0, 0)], "the request must go out with the cursor it had"
    assert c._flow_control_triggered == 0
    assert c._pull_not_before == {}


def test_lite_parked_queue_never_delays_a_cold_sibling(monkeypatch):
    hot = _lite_mq(0)
    cold = _lite_mq(1)
    c, client = _lite_round_consumer("G_lite_round_siblings", [hot, cold])
    c.pull_threshold_for_queue = 1
    _lite_cache(c, [_lite_msg(100, 0, queue_id=0), _lite_msg(100, 1, queue_id=0)])
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=2)
    assert [qid for (qid, _) in client.requests] == [1, 1], \
        "only the cold sibling ever reaches the broker; the hot one is refused every round"
    assert sleeps == [PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL] * 2, sleeps
    assert c._flow_control_triggered == 2, "one tick per refusal, and only for the hot queue"
    assert cold not in c._pull_not_before


def test_lite_gates_recover_once_the_application_polls_dry(monkeypatch):
    mq = _lite_mq()
    c, client = _lite_round_consumer("G_lite_round_recovery", [mq])
    c.pull_threshold_for_queue = 1
    _lite_cache(c, [_lite_msg(100, 0), _lite_msg(100, 1)])
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=3,
                               on_round_end=lambda: c.poll(timeout=0))
    # refused, then the application drains the buffer and the queue pulls again: no deadlock.
    assert sleeps == [PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL, 0.5, 0.5], sleeps
    assert client.requests == [(0, 0), (0, 0)], client.requests
    assert c._flow_control_triggered == 1


def test_lite_cache_pause_and_broker_pause_reuse_the_shared_constants():
    # The lite path must re-schedule with the port's own delays, not invent second values of its own.
    assert PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL == 0.05
    assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
    loop_src = inspect.getsource(DefaultLitePullConsumer._pull_service_loop)
    assert "PULL_TIME_DELAY_WHEN_CACHE_FLOW_CONTROL" in loop_src
    assert "PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL" not in loop_src
    pull_src = inspect.getsource(DefaultLitePullConsumer._pull_one)
    assert "PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL" in pull_src


# ---------------- a broker throttle on this path is not an error ----------------

def test_lite_broker_flow_control_retries_in_20ms_other_codes_keep_the_error_backoff(monkeypatch):
    mq = _lite_mq()
    c, client = _lite_round_consumer(
        "G_lite_broker_215", [mq],
        answers=[MQBrokerException(ResponseCode.FLOW_CONTROL, "broker throttling"),
                 MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")])
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=2)
    # 215 gets the short broker-side re-schedule, any other broker code keeps the lite path's own handling
    # (the idle round backoff here): the two legs pin each other, reverting either branch turns this red.
    assert sleeps == [PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL, 0.5], sleeps
    assert len(client.requests) == 2, "a 215 parks the next round for 20 ms, it does not stop pulling"
    assert c._flow_control_triggered == 0, "the broker's throttle is not this client's cache gate"


def test_lite_plain_pull_error_parks_nobody_and_keeps_the_generic_backoff(monkeypatch):
    # Control without a broker code at all: a generic failure must not pick the 20 ms branch, and it must
    # not leave a queue parked.
    mq = _lite_mq()
    c, client = _lite_round_consumer("G_lite_broker_generic", [mq],
                                     answers=[RuntimeError("socket reset")])
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=1)
    assert sleeps == [0.5], sleeps
    assert c._pull_not_before == {}
    assert client.requests == [(0, 0)]


def test_lite_broker_flow_control_parks_only_the_answered_queue(monkeypatch):
    hot = _lite_mq(0)
    sibling = _lite_mq(1)
    c, client = _lite_round_consumer(
        "G_lite_broker_215_one_queue", [hot, sibling],
        error_on_queue=(0, MQBrokerException(ResponseCode.FLOW_CONTROL, "broker throttling")))
    sleeps = _drive_lite_rounds(monkeypatch, c, rounds=1)
    assert sleeps == [PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL], sleeps
    assert sorted(qid for (qid, _) in client.requests) == [0, 1], \
        "the sibling still pulled in the very round the 215 arrived"
    assert len(client.requests) == 2
    assert list(c._pull_not_before) == [hot], "only the throttled queue is parked for 20 ms"
    assert c._pull_not_before[hot] == PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL


# ---------------- the three counters and the shared 1/1001 warn cadence ----------------

def test_lite_gate_debug_names_the_queue_the_measurement_the_threshold_and_the_counter(caplog):
    c, mq = _lite_with_backlog("G_lite_debug_line", [_lite_msg(100, i) for i in range(2)])
    c.pull_threshold_for_queue = 1
    with caplog.at_level("DEBUG", logger="rocketmq.client"):
        assert _lite_hit(c, mq)
    assert _lite_debugs(caplog) == [
        "flow control: queue %s count=2 exceeds pull_threshold_for_queue=1,"
        " pause pull for 50 ms (flowControlTimes=1)" % mq], _lite_debugs(caplog)


def test_lite_warn_cadence_is_trip_1_and_1001_with_trip_1000_debug_only(caplog):
    assert FLOW_CONTROL_WARN_EVERY_N_TRIPS == 1000
    c, mq = _lite_with_backlog("G_lite_warn_cadence", [_lite_msg(100, i) for i in range(2)])
    c.pull_threshold_for_queue = 1
    with caplog.at_level("WARNING"):
        assert _lite_hit(c, mq)
        assert len(_lite_warns(caplog)) == 1, "the 1st trip must warn"
        assert _lite_hit(c, mq)
        assert len(_lite_warns(caplog)) == 1, "the 2nd trip must stay on the debug channel only"
        # Jump to the boundary and warn again: this pins the modulo, not "warn once and stop".
        c._flow_control_triggered = FLOW_CONTROL_WARN_EVERY_N_TRIPS - 1
        assert _lite_hit(c, mq)
        assert len(_lite_warns(caplog)) == 1, "trip 1000 must stay on the debug channel only"
        assert _lite_hit(c, mq)
        assert len(_lite_warns(caplog)) == 2, "trip 1001 must warn again"
    assert c._flow_control_triggered == FLOW_CONTROL_WARN_EVERY_N_TRIPS + 1
    first = _lite_warns(caplog)[0]
    assert ("The cached message count exceeds the threshold 1, so do flow control, "
            "minOffset=0, maxOffset=1, count=2, size=0 MiB, flowControlTimes=1") == first, first


def test_lite_size_gate_warns_on_the_shared_count_counter(caplog):
    """The count gate and the size gate share one counter, so one throttled queue is one flowControlTimes
    total whichever of the two refused it."""
    c, mq = _lite_with_backlog("G_lite_size_warn", [_lite_msg(2 * MIB, i) for i in range(3)])
    c.pull_threshold_for_queue = 10
    c.pull_threshold_size_for_queue = 1
    with caplog.at_level("WARNING"):
        assert _lite_hit(c, mq)
    assert c._flow_control_triggered == 1
    warn = _lite_warns(caplog)[0]
    assert warn.startswith("The cached message size exceeds the threshold 1 MiB, so do flow control"), warn
    assert "count=3, size=6 MiB, flowControlTimes=1" in warn, warn
    # The next refusal of the same queue ticks the same counter, whatever gate refused it.
    assert _lite_hit(c, mq)
    assert c._flow_control_triggered == 2
    assert len(_lite_warns(caplog)) == 1, "trip 2 stays on the debug channel"


def test_lite_aggregate_gate_warns_on_its_own_counter(caplog):
    queues = [_lite_mq(i) for i in range(2)]
    c = _lite_consumer("G_lite_all_warn", *queues)
    _lite_gates_off(c)
    c.pull_batch_size = 8
    c.pull_threshold_for_all = 1
    _lite_cache(c, [_lite_msg(100, j, queue_id=i) for i in range(2) for j in range(2)])
    with caplog.at_level("WARNING"):
        assert _lite_hit(c, queues[0])
        assert _lite_hit(c, queues[1])
        assert c._flow_control_for_all_triggered == 2
        assert len(_lite_warns(caplog)) == 1, "trip 2 stays on the debug channel"
        c._flow_control_for_all_triggered = FLOW_CONTROL_WARN_EVERY_N_TRIPS
        assert _lite_hit(c, queues[0])
    assert len(_lite_warns(caplog)) == 2, "trip 1001 must warn again"
    # The warn translates the message budget back into rounds: ceil(1 / 8) is 1 round, and 2 are pending.
    assert ("the consume request count exceeds threshold 1, so do flow control,"
            " consume request count=2, flowControlTimes=1") == _lite_warns(caplog)[0], _lite_warns(caplog)[0]


def test_lite_span_gate_warns_on_its_own_counter(caplog):
    c, mq = _lite_with_backlog("G_lite_span_warn", [_lite_msg(1, 0), _lite_msg(1, 11)])
    c.consume_max_span = 10
    with caplog.at_level("WARNING"):
        assert _lite_hit(c, mq)
        assert c._flow_control_span_triggered == 1
        assert c._flow_control_triggered == 0, "the span gate never ticks the count/size counter"
    assert ("The queue's messages, span too long, so do flow control,"
            " minOffset=0, maxOffset=11, maxSpan=11, flowControlTimes=1"
            == _lite_warns(caplog)[0]), _lite_warns(caplog)[0]


def test_lite_three_gates_keep_three_independent_cadences(caplog):
    """Each counter starts its own 1/1001 cadence: driving one of them to the boundary must not make another
    gate's next warn appear or disappear."""
    c, mq = _lite_with_backlog("G_lite_cadence_split",
                               [_lite_msg(2 * MIB, 0), _lite_msg(2 * MIB, 11)])
    c.pull_threshold_for_all = 0
    c.pull_threshold_size_for_queue = 1
    c.consume_max_span = 10
    with caplog.at_level("WARNING"):
        assert _lite_hit(c, mq)                    # aggregate wins
        assert len(_lite_warns(caplog)) == 1
        c.pull_threshold_for_all = OUT_OF_REACH
        assert _lite_hit(c, mq)                    # now the size gate wins
        assert len(_lite_warns(caplog)) == 2, "a different gate ticks its own first trip, so it warns"
        c.pull_threshold_size_for_queue = OUT_OF_REACH
        assert _lite_hit(c, mq)                    # and the span gate wins
        assert len(_lite_warns(caplog)) == 3
    assert (c._flow_control_for_all_triggered, c._flow_control_triggered,
            c._flow_control_span_triggered) == (1, 1, 1)


# ---------------- shutdown escape ----------------

def test_lite_shutdown_stops_promptly_while_a_queue_is_parked_on_a_gate():
    # A live background round parked on the aggregate gate (nothing here ever polls the buffer). Shutdown
    # must not sit out the pause and must not leave the round loop behind.
    store = {("broker-a", 0): ["A%d" % i for i in range(4)]}
    c = _Lite("G_lite_shutdown", _store=store)
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_auto_commit(False)
    mq = MessageQueue("T", "broker-a", 0)
    c.assign([mq])
    c.pull_threshold_for_all = 1
    c.start()
    try:
        began = time.time()
        while c._flow_control_for_all_triggered == 0 and time.time() - began < 10:
            time.sleep(0.01)
        assert c._flow_control_for_all_triggered > 0, "the queue must actually be parked on a gate"
        assert mq in c._pull_not_before
        stopped_in = time.time()
        c.shutdown()
        stopped_in = time.time() - stopped_in
        assert stopped_in < 1.0, "a parked queue must not sit out its pause: shutdown took %.3fs" % stopped_in
        assert not c._pull_thread.is_alive(), "the round loop must be gone after shutdown"
    finally:
        c.shutdown()

