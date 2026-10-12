import pytest

from client.consumer import (MAX_POP_INVISIBLE_TIME,
                                      MIN_POP_INVISIBLE_TIME,
                                      DefaultMQPushConsumer)
from client.exception import MQClientException

GROUP = "CID_check_config_py"
TOPIC = "CheckConfigPyTopic"
# 不可达地址：闸门必须在这之前就把配置判完，所以这条断言顺带证明"没碰网络"。
NAMESRV = "127.0.0.1:1"


def _listener(_msgs):
    return None


def _consumer(**overrides):
    """装配到"只差数值闸门"为止的消费者：组名/地址/订阅/listener 全合法。"""
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

# 下界不是 0、且没有 -1 哨兵的闸门：写 0 必须在启动期报错。
ZERO_ILLEGAL = [f for f, lo, *_ in GATES if lo != 0 and f not in TOPIC_SENTINELS]


class TestDefaultsPass:
    def test_default_config_clears_every_gate(self):
        """默认值必须过闸门：闸门收紧后最容易伤到的就是不改配置的正常使用方。

        这里**不**调 ``start()`` —— 离线环境连不上 name server，``start()`` 会在
        建连那步抛 ``RemotingConnectException``，跟闸门无关。"边界值真机能启动并消费"
        由 ../verify_flow_control_live.py 的 S5 在真集群上锁。
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
        """下界与上界**本身**合法：把 ``< lo`` 写成 ``<= lo`` 只有这一格能抓到。

        ``consume_thread_*`` 要把 min/max 一起放到各自的极值上：默认 (20, 20) 下
        单独抬 min 到 1000 或压 max 到 1 都会先撞上"min 不能大于 max"那道门，
        区间边界这一格就没测到。
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
        """只有 -1 是哨兵：-2 落到区间判断里必须被拒（把守卫写成 ``< 0`` 就在这里变红）。"""
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
        """min 自身越界时报 min 的区间，而不是把两个数字塞进 "is larger than" 文案。"""
        c = _consumer(consume_thread_min=2000, consume_thread_max=1)
        with pytest.raises(MQClientException) as ei:
            c._check_config_ranges()
        assert str(ei.value) == "consumeThreadMin Out of range [1, 1000]"


class TestGateIsLocalAndOrdered:
    def test_no_instance_is_created_when_config_is_bad(self):
        """闸门必须在建 MQClientInstance **之前**：起了后台线程再抛错就泄漏线程了。"""
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
        """改回合法值后同一个实例仍然能过闸门：拒绝启动不能把对象写坏或留下半启动状态。"""
        c = _consumer(pop_batch_nums=33)
        with pytest.raises(MQClientException):
            c.start()
        assert c._started is False and c._mq_client is None
        c.pop_batch_nums = 32
        c._check_config_ranges()
