# ProcessQueue 停摆自愈（Java isPullExpired / PULL_MAX_IDLE_TIME）单测 —— 不需要集群。
#
# 为什么必须离线锁死：这一条判据是**唯一**能把"死掉的拉取循环"救回来的机制。
# Java RebalanceImpl.updateProcessQueueTableInRebalance:438-461 在同一趟 rebalance 里
# 先撤（!mqSet.contains 或 pq.isPullExpired() → setDropped + removeUnnecessaryMessageQueue）
# 再建（新 ProcessQueue）。少第一步，一条循环线程异常退出后那个队列就**永久不再消费**
# ——真机上表现为"某个队列一直不涨位点"，而且没有任何异常；少第二步（撤了不重建），
# 后果同样是永久停摆。两种错都不会让任何一次已完成的投递失败，所以只能靠断言锁住。
#
# 阈值本身也锁在这里：Java ProcessQueue:43 读 `rocketmq.client.pull.pullMaxIdleTime`，
# 默认 **120000ms**（不是网传的 60s），POP 分支换成 lastPopTimestamp（PopProcessQueue:74）。
#
# 与 C++/Rust/.NET 的 test_pull_expired 一一对应；真机恢复场景见 ../verify_pull_expired_live.py
#（注入停摆 → 走生产 rebalance 路径重建 → 同一队列继续消费、位点不回退）。
import threading
import time

from rocketmq.client.consumer import (MIN_POP_INVISIBLE_TIME, DefaultMQPushConsumer,
                                      PopProcessQueue, PULL_MAX_IDLE_TIME)
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.subscription_data import SubscriptionData
from rocketmq.remoting.protocol.heartbeat import MessageModel

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
    """一个"已经跑起来、分到 1 个队列"的消费者：拉取循环换成一具可控的空壳。

    把 target 换成 park（只是 wait），线程保持 alive 但**不碰网络**，这样
    _rebalance_pull_threads 的撤/建分支可以单独验证；盖章由测试自己写
    _last_pull_table，等价于真循环在每轮入口做的事。
    """
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    c.pop_mode = pop_mode
    mq = MessageQueue("TopicTest", "broker-a", 0)
    c._assigned = [mq]
    key = c._mq_key(mq)
    c._queue_pull_loop = lambda m: c._stop.wait(30)      # 不触网，只占住线程
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
    """把 _on_queues_revoked 换成记录器：撤走时的收尾动作是断言的重点，但不能真发 RPC。"""
    seen = []
    c._on_queues_revoked = lambda revoked: seen.extend(revoked)
    return seen


# ---------------- 常量与判据 ----------------

def test_pull_max_idle_time_matches_java_default():
    """Java ProcessQueue:43 的默认值是 120s；写错方向会静默误撤健康队列或永不自愈。"""
    assert PULL_MAX_IDLE_TIME == 120.0


def test_stall_predicate_needs_both_signals():
    """判据：线程死了算停摆；线程活着但超过阈值没盖章也算；没盖过章的新循环不算。"""
    c, mq, key = _consumer()
    try:
        c._queue_threads[key] = _parked_thread(c)
        assert c._pull_stalled_locked(key) is False, "刚起的循环不该被判停摆"

        c._last_pull_table[key] = time.time() - (PULL_MAX_IDLE_TIME - 10)
        assert c._pull_stalled_locked(key) is False, "阈值内不该撤"

        c._last_pull_table[key] = time.time() - STALE
        assert c._pull_stalled_locked(key) is True, "超阈值必须判停摆"

        c._queue_threads[key] = _dead_thread()
        c._last_pull_table[key] = time.time()
        assert c._pull_stalled_locked(key) is True, "线程已退出即刻判停摆，不必等满阈值"
    finally:
        c._stop.set()


def test_stall_predicate_ignores_threads_not_yet_stampable():
    """线程已建、还没跑到盖章处（表里没有时刻）：不算停摆，否则刚分配就被撤。"""
    c, mq, key = _consumer()
    try:
        c._queue_threads[key] = _parked_thread(c)
        assert key not in c._last_pull_table
        assert c._pull_stalled_locked(key) is False
    finally:
        c._stop.set()


# ---------------- 撤 + 建：同一趟里复原 ----------------

def test_stalled_queue_is_retired_and_rebuilt_in_one_pass():
    """核心场景：停摆队列被撤（位点持久化、缓冲丢弃），同一趟立刻换新线程重建。"""
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        old = _parked_thread(c)
        c._queue_threads[key] = old
        c._last_pull_table[key] = time.time() - STALE
        c._mq_map[key] = mq
        c._pending[key] = _msgs(2)              # 已拉未消费的一批
        c._offset_table[key] = 77                 # 拉取游标
        c._consume_offsets[key] = 42              # 已消费位点，必须被持久化

        c._rebalance_pull_threads()

        assert retired == [(mq, 42)], "停摆队列必须按撤走收尾：持久化它的已消费位点"
        new = c._queue_threads[key]
        assert new is not old, "必须换掉那条停摆线程，否则还是死的"
        assert c._pending.get(key) is None, "已拉未消费的缓冲要丢弃，由新循环重投"
        assert c._offset_table.get(key) is None, "拉取游标要丢弃（新循环从 broker 位点重算）"
        assert c._consume_offsets.get(key) is None, "已消费位点交给 broker，本地不留"
        assert 0 <= time.time() - c._last_pull_table[key] < 5, "重建时重新占位，别继承旧时刻"
    finally:
        c._stop.set()


def test_thread_death_is_enough_to_rebuild():
    """Java 的 [BUG] 分支防的就是这个：循环线程被异常打穿，队列还在本实例名下。"""
    c, mq, key = _consumer()
    retired = _retire_log(c)
    try:
        old = _dead_thread()
        c._queue_threads[key] = old
        c._last_pull_table[key] = time.time()     # 盖章还新鲜，只有线程死了
        c._mq_map[key] = mq

        c._rebalance_pull_threads()

        assert c._queue_threads[key] is not old
        assert len(retired) == 1 and retired[0][0] == mq
    finally:
        c._stop.set()


def test_healthy_queue_is_left_alone():
    """没停摆就不许动：换线程等于把在途消息丢弃重投，白增重复消费。"""
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
    """分配即登记 _mq_map（Java ProcessQueueTable 的键集），否则停摆时无 mq 可持久化位点。"""
    c, mq, key = _consumer()
    _retire_log(c)
    try:
        c._rebalance_pull_threads()
        assert c._mq_map[key] == mq
    finally:
        c._stop.set()


def test_pop_expired_uses_last_pop_timestamp_and_drops_pq():
    """POP 分支：判据读 lastPopTimestamp（PopProcessQueue:74-76），撤走时 set_dropped。"""
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

        assert pq.is_dropped() is True, "在途批次必须停：既不消费也不 ack，等 broker 复活"
        assert c._pop_queues[key] is not pq, "重建要换一具干净的 PopProcessQueue"
        assert len(retired) == 1 and retired[0][0] == mq
    finally:
        c._stop.set()


def test_stopped_consumer_is_not_swept():
    """shutdown 期间线程本来就陆续退出：这时不判停摆，否则会刷一堆假的 [BUG] 日志。"""
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
        assert retired == [], "停机不该被当成停摆自愈"
        assert c._consume_offsets[key] == 7
    finally:
        c._stop.set()


# ---------------- 盖章时机：发起即盖章 ----------------

def test_pull_loop_stamps_before_lock_and_flow_control():
    """盖章在流控/锁判定**之前**（Java pullMessage:253 的位置）：卡住的循环也要留下心跳。

    若把盖章挪到流控之后，一次持续超 120s 的流控会让 rebalance 误判停摆并撤队列，
    把"消费慢"处理成"重投一遍"。这里用必触发流控的消费者验证：一次网络都没打，
    时刻却一直在前进。
    """
    c, mq, key = _consumer()
    pulls = []

    class _Client(object):
        def pull_message(self, *args, **kwargs):
            pulls.append(1)
            raise AssertionError("must not reach the network")

    c._mq_client = _Client()
    c.subscription_data = {mq.topic: _sub()}
    del c._queue_pull_loop                      # 用真循环，别用 _consumer() 的空壳
    c.pull_threshold_for_queue = 1
    c._pending[key] = _msgs(3)                  # 已拉 3 条 >= 阈值 1 → 每轮都触发流控
    c._offset_table[key] = 0
    try:
        th = threading.Thread(target=c._queue_pull_loop, args=(mq,), daemon=True)
        c._queue_threads[key] = th              # 循环靠 _owns_queue 认主，先登记再起
        th.start()
        deadline = time.time() + 3
        while time.time() < deadline and key not in c._last_pull_table:
            time.sleep(0.05)
        c._stop.set()
        th.join(timeout=3)
        assert key in c._last_pull_table, "流控期间也必须盖章"
        assert pulls == [], "触发流控就不该打网络"
        assert len(c._pending[key]) == 3, "流控只暂停拉取，不动缓冲"
    finally:
        c._stop.set()


def test_pop_loop_stamps_before_flow_control():
    """POP 同理：入口同时更新 _last_pull_table 和 PopProcessQueue.lastPopTimestamp。"""
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
    c.pop_threshold_for_queue = 1               # waitAck 5 > 1 → 每轮都触发流控
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
        assert pq.last_pop_timestamp > old_stamp, "弹不出去也要更新 lastPopTimestamp"
        assert key in c._last_pull_table
        assert pops == []
    finally:
        c._stop.set()


# ---------------- 运行信息：307 要能看见时刻 ----------------

def test_running_info_reports_real_last_pull_timestamp():
    """Java ProcessQueue.fillOutRunningInfo:456 报 lastPullTimestamp。

    写死 0 等于把停摆判据的现场证据全丢了：运维查 307 时看不到"这路多久没拉了"。
    """
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
    from rocketmq.client.consumer import MessageModel

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
