# 与其它语言同构：go/client/consumer_test.go 的
# TestConsumerSuspendStopsPullsAndResumeRestartsThem、
# csharp/tests/RocketMQ.Client.Tests/SuspendResumeTests.cs、
# nodeJs/test/java_gap_fill_smoke.ts 的挂起块。
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
    """只记账的 broker 替身：应答一律"没有新消息"，永远不真投递。

    记账用**发起时刻**而不是次数，这样同一次断言既能问"挂起后还有没有新报文"，
    也能问"挂起期间盖章有没有继续前进"。

    每次应答都睡 LONG_POLL：真 broker 的长轮询会挂住这一笔请求，替身不减速就成了
    每秒数万笔的空转 —— suspend() 与"取基线"之间那几毫秒的窗口会挤进几百笔拉取，
    断言就变成竞态。30ms 一笔时该窗口最多 1 笔，且已由取基线前的 settle 消化掉。
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
        """本替身见过的报文数：pull/pop 两条路径各记各的表，取和即可。"""
        return len(self.pull_at) + len(self.pop_at)


def _consumer(pop_mode=False):
    """一个"已启动、分到 1 个队列"的消费者，配一具记账替身，不碰真实网络。"""
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    c.pop_mode = pop_mode
    fake = _FakeClient()
    c._mq_client = fake
    mq = MessageQueue(TOPIC, BROKER, 0)
    c._assigned = [mq]
    c.subscription_data = {TOPIC: SubscriptionData(topic=TOPIC, sub_string="*")}
    key = c._mq_key(mq)
    c._offset_table[key] = 0          # 位点已知，跳过 _resolve_initial_offset
    c._mq_map[key] = mq
    return c, mq, key, fake


def _run_loop(c, mq, key, pop_mode):
    """起真循环线程（_owns_queue 靠线程表认主，先登记再起）。"""
    target = c._queue_pop_loop if pop_mode else c._queue_pull_loop
    th = threading.Thread(target=target, args=(mq,), daemon=True)
    c._queue_threads[key] = th
    th.start()
    return th


def _wait_pulls(fake, at_least, timeout=3.0):
    """等到替身见过 at_least 笔报文为止（pull/pop 两条路径共用）。"""
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
    assert c.is_paused() is False, "默认不挂起：pause 初始为 false"


# ---------------- 核心：挂起停拉取，恢复接着拉 ----------------

def test_suspend_stops_pulls_and_resume_restarts_them():
    """挂起后不许再发 PULL_MESSAGE，恢复后必须自己重新发起（不需要重建线程）。"""
    c, mq, key, fake = _consumer()
    th = _run_loop(c, mq, key, pop_mode=False)
    try:
        assert _wait_pulls(fake, 2), "没先跑出几笔拉取，'停了'就没有说服力"
        c.suspend()
        assert c.is_paused() is True
        _settle()                     # 让挂起前已在途的那一笔先落地

        baseline = fake.count()
        stamp_before = c._last_pull_table[key]
        time.sleep(2.5)

        assert fake.count() == baseline, "挂起期间不能有新的 PULL_MESSAGE 上线"
        assert c._last_pull_table[key] > stamp_before, \
            "挂起期间 lastPullTimestamp 必须继续推进：闸门在盖章之后，" \
            "否则一次超过 120s 的挂起会被停摆判据当成死循环，rebalance 把分配整个摘走"
        with c._lock:
            assert c._pull_stalled_locked(key) is False, "挂起不等于停摆"
        assert c._queue_threads[key] is th, "挂起不换线程、不撤队列：只翻一个布尔"
        assert c._mq_map.get(key) == mq, "分配关系保持原样"

        c.resume()
        assert c.is_paused() is False
        assert _wait_pulls(fake, baseline + 1, timeout=5.0), "恢复后必须自己重新开始拉"
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
        _settle()                              # 挂起前那一笔已落地
        cursor_before = c._offset_table[key]
        time.sleep(1.2)
        assert list(c._pending[key]) == buffered, "已拉到本地的消息要照常留在缓冲里"
        assert c._offset_table[key] == cursor_before, "挂起不改拉取游标"
    finally:
        _stop(c, th)


def test_resume_triggers_rebalance():
    pass
    c, mq, key, fake = _consumer()
    try:
        c.suspend()
        c._rebalance_now.clear()
        c.resume()
        assert c._rebalance_now.is_set() is True, "resume 必须叫醒重平衡循环"
    finally:
        c._paused = False


# ---------------- POP 模式：同一道闸门 ----------------

def test_pop_mode_suspend_stops_pops_and_keeps_stamping():
    pass
    c, mq, key, fake = _consumer(pop_mode=True)
    pq = PopProcessQueue()
    c._pop_queues[key] = pq
    c.pop_threshold_for_queue = 1000
    th = _run_loop(c, mq, key, pop_mode=True)
    try:
        assert _wait_pulls(fake, 2), "没先跑出几笔弹出，'停了'就没有说服力"
        c.suspend()
        _settle()                              # 在途那一笔先落地

        baseline = fake.count()
        stamp_before = pq.last_pop_timestamp
        table_before = c._last_pull_table[key]
        time.sleep(2.5)

        assert fake.count() == baseline, "挂起期间不能有新的 POP_MESSAGE 上线"
        assert pq.last_pop_timestamp > stamp_before, "lastPopTimestamp 必须继续推进"
        assert c._last_pull_table[key] > table_before
        with c._lock:
            assert c._pull_stalled_locked(key) is False, "挂起不等于停摆，120s 判据不该命中"
        assert pq.is_dropped() is False, "挂起不撤队列"

        c.resume()
        assert _wait_pulls(fake, baseline + 1, timeout=5.0), "恢复后必须自己重新开始弹"
    finally:
        _stop(c, th)


# ---------------- 幂等 / 未启动也安全 ----------------

def test_suspend_and_resume_are_idempotent_and_safe_before_start():
    pass
    c = DefaultMQPushConsumer("G_suspend_idle")
    assert c.is_paused() is False
    c.suspend()
    c.suspend()
    assert c.is_paused() is True, "重复 suspend 必须保持挂起态"
    c.resume()
    c.resume()
    assert c.is_paused() is False, "重复 resume 必须保持运行态"


def test_paused_consumer_is_not_swept_by_rebalance():
    """把两条判据连起来看：挂起的队列在重平衡里必须**原封不动**。

    这是真机上最容易踩的组合：suspend() 后运维通常离开一段时间，回来 resume()，
    如果这期间重平衡撤了队列，恢复时看到的就是重投过的消息。
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
        assert retired == [], "挂起的队列不该被撤（撤走会持久化位点并丢弃缓冲）"
        assert c._queue_threads[key] is th
    finally:
        _stop(c, th)


def test_stall_window_is_far_longer_than_the_suspend_backoff():
    """两个常量的相对关系：1s 退避 vs 120s 停摆阈值。

    挂起靠"每 1s 盖一次章"活过停摆判据；反过来若退避时长写成了 150s，
    挂起 = 停摆，第一个用例里那条断言就守不住真实场景了。
    """
    assert PULL_TIME_DELAY_WHEN_SUSPEND < PULL_MAX_IDLE_TIME / 2
    assert PULL_MAX_IDLE_TIME == 120.0
