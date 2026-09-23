# 推送消费者配置数值闸门（Java DefaultMQPushConsumerImpl#checkConfig :1099-1209）单测
# —— 不需要集群。
#
# 为什么必须离线锁死：这十三道闸门在 Java 里是**启动期拒绝**，而每条的错法都是
# 运行期**静默**的：
#   - ``pullThreshold* = 0`` 走的是 ``max(1, n)`` 兜底 ⇒ 每条消息都算超限，队列永久停拉，
#     消费端一条都收不到，且日志里没有任何异常；
#   - ``consumeThreadMax`` 是 ``update_core_pool_size`` 的上界守卫，写成 0 之后
#     弹性调节永远返回 False；
#   - ``popBatchNums > 32`` broker 直接回 INVALID_PARAMETER，POP 循环退化成错误重试；
#   - 巨值（``2**31-1``）绕开 drop/流控判断，堆内存到 OOM 才看得见。
# 区间**上下界都要测**：Java 全是 ``< lo || > hi`` 严格不等，把 ``>`` 写成 ``>=``
# 只会让"边界值本身"这一格变红，而只测下界则完全看不出上界写歪。
#
# 本文件锁的是**参考实现**本身（Python 是四语言的对齐基准），C++/Rust/.NET 的
# 同名用例与这里逐条同构。文案也在这里钉住：排障时运维只看错误串，
# Java 的字段名（camelCase）与区间必须一字不差，否则对照不上文档。
import pytest

from rocketmq.client.consumer import (MAX_POP_INVISIBLE_TIME,
                                      MIN_POP_INVISIBLE_TIME,
                                      DefaultMQPushConsumer)
from rocketmq.client.exception import MQClientException

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


# (字段, 下界, 上界, Java 文案)；``popBatchNums`` 在 Java 写的是 ``<= 0``，对整数同义。
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

# Java 的 ``!= -1`` 哨兵：这两个允许 -1（"未设置，用 queue 级阈值"）。
TOPIC_SENTINELS = ["pull_threshold_for_topic", "pull_threshold_size_for_topic"]

# 下界不是 0、且没有 -1 哨兵的闸门：写 0 必须在启动期报错。
ZERO_ILLEGAL = [f for f, lo, *_ in GATES if lo != 0 and f not in TOPIC_SENTINELS]


class TestDefaultsPass:
    def test_java_defaults_clear_every_gate(self):
        """默认值必须过闸门：闸门收紧后最容易伤到的就是不改配置的正常使用方。

        这里**不**调 ``start()`` —— 离线环境连不上 name server，``start()`` 会在
        建连那步抛 ``RemotingConnectException``，跟闸门无关。"边界值真机能启动并消费"
        由 ../verify_flow_control_live.py 的 S5 在真集群上锁。
        """
        c = _consumer()
        c._check_config_ranges()
        # 默认值本身就是 Java 的那一组，改默认会连带 #72 一起对不上
        assert (c.consume_thread_min, c.consume_thread_max) == (20, 64)
        assert c.consume_concurrently_max_span == 2000
        assert (c.pull_threshold_for_queue, c.pull_threshold_size_for_queue) == (1000, 100)
        assert (c.pull_threshold_for_topic, c.pull_threshold_size_for_topic) == (-1, -1)
        assert (c.pull_interval, c.consume_message_batch_max_size, c.pull_batch_size) == (0, 1, 32)
        assert (c.pop_invisible_time, c.pop_batch_nums) == (60000, 32)

    @pytest.mark.parametrize("field,lo,hi,_msg", GATES)
    def test_both_bounds_are_inclusive(self, field, lo, hi, _msg):
        """下界与上界**本身**合法：把 ``< lo`` 写成 ``<= lo`` 只有这一格能抓到。

        ``consume_thread_*`` 要把 min/max 一起放到各自的极值上：默认 (20, 64) 下
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
    def test_zero_is_rejected_where_java_says_off_is_not_a_number(self, field):
        """``0`` 不是"关闭"的写法：Java 只给两个 topic 级阈值留了 -1 哨兵，其余一律区间内。

        本仓库运行期另有 ``max(1, n)`` / ``> 0`` 兜底（Java 同样有），但那道兜底**不能**
        替代启动期闸门 —— 闸门保证"写 0 的人当场收到错误"，兜底只服务运行期热改。
        """
        with pytest.raises(MQClientException):
            _consumer(**{field: 0})._check_config_ranges()

    @pytest.mark.parametrize("field", TOPIC_SENTINELS)
    def test_minus_one_sentinel_is_accepted(self, field):
        """-1 = 沿用 queue 级阈值，Java 的 ``!= -1`` 分支必须放行，否则默认配置自己就起不来。"""
        _consumer(**{field: -1})._check_config_ranges()

    @pytest.mark.parametrize("field", TOPIC_SENTINELS)
    def test_minus_two_is_not_a_sentinel(self, field):
        """只有 -1 是哨兵：-2 落到区间判断里必须被拒（把守卫写成 ``< 0`` 就在这里变红）。"""
        with pytest.raises(MQClientException):
            _consumer(**{field: -2})._check_config_ranges()


class TestThreadMinNotLargerThanMax:
    def test_min_larger_than_max_is_rejected(self):
        """两道区间各自都合法，但 min > max 仍然不行：Java 单独一条、**不带** FAQ 后缀。"""
        c = _consumer(consume_thread_min=64, consume_thread_max=32)
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert str(ei.value) == "consumeThreadMin (64) is larger than consumeThreadMax (32)"

    def test_equal_min_and_max_is_fine(self):
        """Java 用的是严格 ``>``：min == max 是合法的单线程池配置。"""
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
        """闸门按 Java 的顺序跑：同时写坏两道时，报的是靠前的那条。

        顺序不是风格问题 —— 一次只吐一个错才修得动，全量收集反而把真正的阻塞项埋掉。
        """
        c = _consumer(consume_thread_max=1001, pull_batch_size=0)
        with pytest.raises(MQClientException) as ei:
            c._check_config_ranges()
        assert str(ei.value) == "consumeThreadMax Out of range [1, 1000]"

    def test_group_and_subscription_checks_still_run_first(self):
        """数值闸门不抢 Java 的 cheque 顺序：组名非法时报组名，即使数值也同时写坏。"""
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
