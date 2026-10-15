import pytest

from client.consumer import (MAX_POP_INVISIBLE_TIME,
                                      MIN_POP_INVISIBLE_TIME,
                                      DefaultMQPushConsumer)
from client.exception import MQClientException

GROUP = "CID_check_config_py"
TOPIC = "CheckConfigPyTopic"
# Unreachable address: the gates must finish judging the config before this, so this assertion incidentally proves "no network was touched".
NAMESRV = "127.0.0.1:1"


def _listener(_msgs):
    return None


def _consumer(**overrides):
    """A consumer assembled up to "only the numeric gates remain": group name / address / subscription / listener are all valid."""
    c = DefaultMQPushConsumer(GROUP)
    c.set_namesrv_addr(NAMESRV)
    c.subscribe(TOPIC)
    c.set_message_listener(_listener)
    for k, v in overrides.items():
        setattr(c, k, v)
    return c


GATES = [
    ("consume_thread_min", 1, 1000, "consumeThreadMin Out of range [1, 1000]"),
    ("consume_thread_max", 1, 1000, "consumeThreadMax Out of range [1, 1000]"),
    ("consume_concurrently_max_span", 1, 65535,
     "consumeConcurrentlyMaxSpan Out of range [1, 65535]"),
    ("pull_threshold_for_queue", 1, 65535, "pullThresholdForQueue Out of range [1, 65535]"),
    ("pull_threshold_for_topic", 1, 6553500, "pullThresholdForTopic Out of range [1, 6553500]"),
    ("pull_threshold_size_for_queue", 1, 1024,
     "pullThresholdSizeForQueue Out of range [1, 1024]"),
    ("pull_threshold_size_for_topic", 1, 102400,
     "pullThresholdSizeForTopic Out of range [1, 102400]"),
    ("pull_interval", 0, 65535, "pullInterval Out of range [0, 65535]"),
    ("consume_message_batch_max_size", 1, 1024,
     "consumeMessageBatchMaxSize Out of range [1, 1024]"),
    ("pull_batch_size", 1, 1024, "pullBatchSize Out of range [1, 1024]"),
    ("pop_invisible_time", MIN_POP_INVISIBLE_TIME, MAX_POP_INVISIBLE_TIME,
     "popInvisibleTime Out of range [%d, %d]" % (MIN_POP_INVISIBLE_TIME, MAX_POP_INVISIBLE_TIME)),
    ("pop_batch_nums", 1, 32, "popBatchNums Out of range [1, 32]"),
]

TOPIC_SENTINELS = ["pull_threshold_for_topic", "pull_threshold_size_for_topic"]

# Gates whose lower bound is not 0 and which have no -1 sentinel: writing 0 must error at startup.
ZERO_ILLEGAL = [f for f, lo, *_ in GATES if lo != 0 and f not in TOPIC_SENTINELS]


class TestDefaultsPass:
    def test_default_config_clears_every_gate(self):
        """Defaults must clear every gate: after gates are tightened, the easiest casualties are normal users who do not change config.

        ``start()`` is deliberately **not** called here — an offline environment cannot reach the name server, and ``start()`` would
        throw ``RemotingConnectException`` at the connection step, which is unrelated to the gates. "Boundary values can really start and consume"
        is locked by S5 of ../verify_flow_control_live.py on a real cluster.
        """
        c = _consumer()
        c._check_config_ranges()
        assert (c.consume_thread_min, c.consume_thread_max) == (20, 20)
        assert c.consume_concurrently_max_span == 2000
        assert (c.pull_threshold_for_queue, c.pull_threshold_size_for_queue) == (1000, 100)
        assert (c.pull_threshold_for_topic, c.pull_threshold_size_for_topic) == (-1, -1)
        assert (c.pull_interval, c.consume_message_batch_max_size, c.pull_batch_size) == (0, 1, 32)
        assert (c.pop_invisible_time, c.pop_batch_nums) == (60000, 32)

    @pytest.mark.parametrize("field,lo,hi,_msg", GATES)
    def test_both_bounds_are_inclusive(self, field, lo, hi, _msg):
        """The lower and upper bounds **themselves** are legal: writing ``< lo`` as ``<= lo`` is only catchable in this cell.

        ``consume_thread_*`` must place min/max at their respective extremes together: under the default (20, 20),
        raising min alone to 1000 or pushing max alone to 1 would first hit the "min must not exceed max" gate,
        and this range-boundary cell would go untested.
        """
        base = ({"consume_thread_min": 1, "consume_thread_max": 1000}
                if field.startswith("consume_thread") else {})
        for bound in (lo, hi):
            overrides = dict(base)
            overrides[field] = bound
            _consumer(**overrides)._check_config_ranges()


class TestOutOfRangeRejected:
    @pytest.mark.parametrize("field,lo,hi,msg", GATES)
    def test_below_lower_bound(self, field, lo, hi, msg):
        c = _consumer(**{field: lo - 1})
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert str(ei.value) == msg

    @pytest.mark.parametrize("field,lo,hi,msg", GATES)
    def test_above_upper_bound(self, field, lo, hi, msg):
        c = _consumer(**{field: hi + 1})
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert str(ei.value) == msg

    @pytest.mark.parametrize("field", ZERO_ILLEGAL)
    def test_zero_is_rejected_as_not_a_number(self, field):
        pass
        with pytest.raises(MQClientException):
            _consumer(**{field: 0})._check_config_ranges()

    @pytest.mark.parametrize("field", TOPIC_SENTINELS)
    def test_minus_one_sentinel_is_accepted(self, field):
        pass
        _consumer(**{field: -1})._check_config_ranges()

    @pytest.mark.parametrize("field", TOPIC_SENTINELS)
    def test_minus_two_is_not_a_sentinel(self, field):
        """Only -1 is a sentinel: -2 must be rejected by the range check (writing the guard as ``< 0`` turns red right here)."""
        with pytest.raises(MQClientException):
            _consumer(**{field: -2})._check_config_ranges()


class TestThreadMinNotLargerThanMax:
    def test_min_larger_than_max_is_rejected(self):
        pass
        c = _consumer(consume_thread_min=64, consume_thread_max=32)
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert str(ei.value) == "consumeThreadMin (64) is larger than consumeThreadMax (32)"

    def test_equal_min_and_max_is_fine(self):
        pass
        _consumer(consume_thread_min=1, consume_thread_max=1)._check_config_ranges()
        _consumer(consume_thread_min=32, consume_thread_max=32)._check_config_ranges()

    def test_the_rule_is_checked_after_both_ranges(self):
        """When min itself is out of range, report min's range instead of stuffing both numbers into the "is larger than" message."""
        c = _consumer(consume_thread_min=2000, consume_thread_max=1)
        with pytest.raises(MQClientException) as ei:
            c._check_config_ranges()
        assert str(ei.value) == "consumeThreadMin Out of range [1, 1000]"


class TestGateIsLocalAndOrdered:
    def test_no_instance_is_created_when_config_is_bad(self):
        """The gates must run **before** building MQClientInstance: throwing after background threads start would leak threads."""
        c = _consumer(pull_batch_size=1025)
        with pytest.raises(MQClientException):
            c.start()
        assert c._started is False
        assert c._mq_client is None

    def test_the_bad_number_reports_itself_first(self):
        pass
        c = _consumer(consume_thread_max=1001, pull_batch_size=0)
        with pytest.raises(MQClientException) as ei:
            c._check_config_ranges()
        assert str(ei.value) == "consumeThreadMax Out of range [1, 1000]"

    def test_group_and_subscription_checks_still_run_first(self):
        pass
        c = DefaultMQPushConsumer("bad group!!")
        c.set_namesrv_addr(NAMESRV)
        c.subscribe(TOPIC)
        c.set_message_listener(_listener)
        c.pull_batch_size = 0
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert "group" in str(ei.value).lower()

    def test_fixing_the_number_lets_the_same_object_start(self):
        """After fixing back to a legal value, the same instance still clears the gates: refusing to start must not corrupt the object or leave a half-started state."""
        c = _consumer(pop_batch_nums=33)
        with pytest.raises(MQClientException):
            c.start()
        assert c._started is False and c._mq_client is None
        c.pop_batch_nums = 32
        c._check_config_ranges()
