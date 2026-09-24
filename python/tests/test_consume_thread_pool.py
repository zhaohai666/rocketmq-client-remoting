# -*- coding: utf-8 -*-
"""消费线程弹性单测（对应 Java DefaultMQPushConsumer 的线程池配置与 updateCorePoolSize）。

为什么需要它：Java 的 ``ThreadPoolExecutor`` 在 ``LinkedBlockingQueue``（无界）下，
**真实并发度 == corePoolSize**（``max`` 永远用不到），所以 ``updateCorePoolSize`` 是
"运行时真的能改并发度"的 API，而标准的 ``concurrent.futures.ThreadPoolExecutor``
**没有 core 这个概念** —— 用它就等于把这个能力做没了。因此本项目自己实现
``ConsumeExecutor``（core/max 两档），本文件把它的语义和 Java 守卫逐条钉住。

另外钉住一个反直觉的事实：**Java 5.5.1 的自动弹性（inc/decCorePoolSize）是空实现**，
``adjustThreadPool()`` 整套调度是 no-op。我们照抄 no-op，不允许"顺手修好"。

覆盖：
  - ConsumeExecutor：core 内建线程 / 超 core 入队 / 提升 core 补线程 /
    超编线程空闲退出而 core 线程不退 / 任务异常不杀 worker / shutdown 语义
  - 消费者：Java 默认值、update_core_pool_size 的五条守卫边界、get_core_pool_size
  - msgAccCnt（MAX_OFFSET - queueOffset）与 compute_accumulation_total
  - adjust_thread_pool 的 inc/dec 确实是 no-op（改阈值也不动 core）
  - 307 运行信息里的 PROP_THREADPOOL_CORE_SIZE 取的是 core pool size
"""
from __future__ import annotations

import threading
import time

import pytest

from rocketmq.client.consume_executor import ConsumeExecutor
from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol.body import ConsumerRunningInfo

GROUP = "GID_ThreadPoolUnit"


# ------------------------------------------------------------------ ConsumeExecutor

class TestConsumeExecutor:
    def test_spawns_up_to_core_then_queues(self):
        """core=2：前两个任务各起一个线程；第 3 个任务只入队，不再建线程。"""
        ex = ConsumeExecutor(2, 8, keep_alive_seconds=30)
        started = threading.Event()
        release = threading.Event()

        def block(tag):
            started.set()
            release.wait(5)

        try:
            ex.submit(block, "a")
            assert started.wait(3)
            started.clear()
            ex.submit(block, "b")
            assert started.wait(3)
            # 两个 worker 都被占住，第 3 个任务只能在队列里等
            ex.submit(block, "c")
            time.sleep(0.1)
            assert ex.worker_count() == 2
            assert ex.queued_count() == 1
        finally:
            release.set()
            ex.shutdown(wait=True)

    def test_raising_core_spawns_for_queued_tasks(self):
        """set_core_pool_size 变大且队列非空 → 立刻补足 worker（Java setCorePoolSize）。"""
        ex = ConsumeExecutor(1, 8, keep_alive_seconds=30)
        release = threading.Event()
        first_started = threading.Event()

        def block():
            first_started.set()
            release.wait(5)

        try:
            ex.submit(block)
            assert first_started.wait(3)
            for _ in range(3):
                ex.submit(lambda: None)
            assert ex.worker_count() == 1
            assert ex.queued_count() == 3
            ex.set_core_pool_size(4)
            deadline = time.time() + 3
            while ex.worker_count() < 4 and time.time() < deadline:
                time.sleep(0.02)
            assert ex.worker_count() == 4
        finally:
            release.set()
            ex.shutdown(wait=True)

    def test_extra_thread_retires_after_keep_alive_core_thread_does_not(self):
        """> core 的线程空闲到 keep_alive 就退出；<= core 的线程永不退出。"""
        ex = ConsumeExecutor(1, 4, keep_alive_seconds=0.2)
        try:
            ex.submit(lambda: None)
            ex.set_core_pool_size(2)          # 队列空 → 不补线程，core 现在是 2
            time.sleep(0.05)
            assert ex.get_core_pool_size() == 2
            # 此刻只有 1 个 worker；再提交任务把它抬到 2（仍 <= core，应在超时后存活）
            ex.submit(lambda: None)
            time.sleep(0.05)
            assert ex.worker_count() == 2
            ex.set_core_pool_size(1)          # core 降到 1 → 多出的那个属于"超编"
            time.sleep(0.6)                   # 超过 keep_alive
            assert ex.worker_count() == 1     # 超编线程已退出
            time.sleep(0.4)
            assert ex.worker_count() == 1     # core 内线程不会退出
        finally:
            ex.shutdown(wait=True)

    def test_task_exception_does_not_kill_worker(self):
        ex = ConsumeExecutor(1, 2, keep_alive_seconds=30)
        done = threading.Event()
        try:
            def boom():
                raise ValueError("boom")

            ex.submit(boom)
            time.sleep(0.2)
            assert ex.handler_exception_count() == 1
            assert ex.worker_count() == 1
            ex.submit(done.set)
            assert done.wait(3)               # 同一个 worker 还能继续干活
        finally:
            ex.shutdown(wait=True)

    def test_submit_after_shutdown_raises(self):
        ex = ConsumeExecutor(1, 2)
        ex.shutdown()
        with pytest.raises(RuntimeError):
            ex.submit(lambda: None)

    def test_shutdown_wait_drains_queued_tasks(self):
        """Java shutdown() 不丢已提交任务：wait=True 要等队列跑完。"""
        ex = ConsumeExecutor(1, 2, keep_alive_seconds=30)
        counter = {"n": 0}
        lock = threading.Lock()

        def work():
            time.sleep(0.02)
            with lock:
                counter["n"] += 1

        for _ in range(5):
            ex.submit(work)
        ex.shutdown(wait=True)
        assert counter["n"] == 5

    def test_zero_core_means_run_to_completion_style_queue(self):
        """core=0（不建常驻线程）时任务仍然会被执行（提交时按需起线程）。"""
        ex = ConsumeExecutor(0, 2, keep_alive_seconds=0.1)
        done = threading.Event()
        try:
            ex.submit(done.set)
            assert done.wait(3)
        finally:
            ex.shutdown(wait=True)


# ------------------------------------------------------------------ 消费者侧

class TestConsumerDefaultsAndGuards:
    def test_java_defaults(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c.get_consume_thread_min() == 20          # Java consumeThreadMin 默认 20 (:162)
        assert c.get_consume_thread_max() == 20          # Java consumeThreadMax 默认 20 (:169)
        assert c.get_adjust_thread_pool_nums_threshold() == 100000
        assert c.get_core_pool_size() == 20              # = consumeThreadMin

    def test_update_core_pool_size_happy_path(self):
        """Java 用无界队列 ⇒ 真实并发度 == core；默认配置下 core 只能往**下**调。"""
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(15) is True
        assert c.get_core_pool_size() == 15

    def test_update_core_pool_size_rejects_zero_and_negative(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(0) is False
        assert c.update_core_pool_size(-1) is False
        assert c.get_core_pool_size() == 20              # 未生效

    def test_update_core_pool_size_rejects_above_short_max(self):
        """Java 守卫 corePoolSize <= Short.MAX_VALUE(32767)。

        注意两道守卫是 **与** 关系：要让 Short.MAX_VALUE 成为实际卡点，必须先把
        consumeThreadMax 抬到 32767 以上，否则先被 ``n < consumeThreadMax`` 拒掉。
        """
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_max(40000)
        assert c.update_core_pool_size(32768) is False
        assert c.update_core_pool_size(32767) is True    # 上界本身是合法的
        assert c.get_core_pool_size() == 32767

    def test_update_core_pool_size_rejects_equal_to_consume_thread_max(self):
        """Java 守卫 corePoolSize < consumeThreadMax —— **等于**也不行。"""
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(20) is False      # == consumeThreadMax（5.x 默认）
        assert c.update_core_pool_size(19) is True
        assert c.get_core_pool_size() == 19

    def test_default_max_is_java_5x_twenty_not_four_x_sixty_four(self):
        """回归：默认 max 曾照抄 4.x 的 64，于是 20~63 这些 Java 会**忽略**的值能生效。

        Java 5.5.1 的 consumeThreadMax 与 min 同为 20（DefaultMQPushConsumer:169），
        守卫 ``n < consumeThreadMax`` 因此把默认配置下的上调全挡掉 —— 差异不会报错，
        只表现为"同一个 update_core_pool_size(30)，本端口真的改了并发度、Java 没改"。
        """
        c = DefaultMQPushConsumer(GROUP)
        assert c.get_consume_thread_max() == 20
        assert c.update_core_pool_size(30) is False
        assert c.update_core_pool_size(21) is False
        assert c.get_core_pool_size() == 20              # 一个都没落下去
        # 抬 max 之后区间重新打开（setter 与 Java 一样是裸赋值，只夹 >= 1）
        c.set_consume_thread_max(30)
        assert c.update_core_pool_size(25) is True
        assert c.get_core_pool_size() == 25

    def test_set_consume_thread_nums_sets_min_max_and_core(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_nums(4)
        assert c.get_consume_thread_min() == 4
        assert c.get_consume_thread_max() == 4
        assert c.get_core_pool_size() == 4
        # max 现在是 4 → 守卫按新 max 判定
        assert c.update_core_pool_size(4) is False
        assert c.update_core_pool_size(3) is True

    def test_consume_thread_min_setter_moves_core(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_min(8)
        assert c.get_consume_thread_min() == 8
        assert c.get_core_pool_size() == 8
        c.set_consume_thread_max(16)
        assert c.get_consume_thread_max() == 16
        assert c.get_core_pool_size() == 8               # 改 max 不动 core

    def test_sizes_are_clamped_to_at_least_one(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_min(0)
        c.set_consume_thread_max(-5)
        assert c.get_consume_thread_min() == 1
        assert c.get_consume_thread_max() == 1

    def test_threshold_setter(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_adjust_thread_pool_nums_threshold(0)
        assert c.get_adjust_thread_pool_nums_threshold() == 0

    def test_executor_is_owned_so_guard_passes(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c._owns_consume_executor is True
        assert c.update_core_pool_size(15) is True


# ------------------------------------------------------------------ msgAccCnt / 阈值

def _msg(queue_offset: int, max_offset=None) -> MessageExt:
    m = MessageExt(topic="T", body=b"x")
    m.queue_offset = queue_offset
    if max_offset is not None:
        m.properties[MessageConst.PROPERTY_MAX_OFFSET] = str(max_offset)
    return m


class TestMsgAccCnt:
    def test_acc_cnt_is_max_offset_minus_queue_offset(self):
        """Java ProcessQueue:148-158 —— accTotal = MAX_OFFSET - queueOffset，取最后一条。"""
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(10, 100), _msg(12, 100)])
        assert c.msg_acc_cnt("k1") == 88                 # 100 - 12

    def test_acc_cnt_ignores_non_positive(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(100, 100)])     # == 0 → 不更新
        assert c.msg_acc_cnt("k1") == 0
        c._update_msg_acc_cnt("k1", [_msg(200, 100)])     # < 0 → 不更新
        assert c.msg_acc_cnt("k1") == 0

    def test_acc_cnt_missing_property_is_ignored(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(10)])           # 没有 MAX_OFFSET 属性
        assert c.msg_acc_cnt("k1") == 0

    def test_acc_cnt_garbage_property_is_ignored(self):
        c = DefaultMQPushConsumer(GROUP)
        m = _msg(10)
        m.properties[MessageConst.PROPERTY_MAX_OFFSET] = "not-a-number"
        c._update_msg_acc_cnt("k1", [m])
        assert c.msg_acc_cnt("k1") == 0

    def test_acc_cnt_keeps_largest_snapshot_of_last_pull(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(10, 500)])
        c._update_msg_acc_cnt("k1", [_msg(400, 450)])     # 新的一批，积压变小
        assert c.msg_acc_cnt("k1") == 50

    def test_compute_accumulation_total_sums_all_queues(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("a", [_msg(0, 30)])
        c._update_msg_acc_cnt("b", [_msg(0, 70)])
        assert c.compute_accumulation_total() == 100
        assert c.msg_acc_cnt() == 100


class TestAdjustThreadPoolIsNoOp:
    def test_inc_branch_does_not_change_core_pool_size(self):
        """acc >= 1.0 * threshold → Java 调 incCorePoolSize()，而它是空实现。"""
        c = DefaultMQPushConsumer(GROUP)
        c.set_adjust_thread_pool_nums_threshold(100)
        c._update_msg_acc_cnt("a", [_msg(0, 500)])        # 500 >= 100
        before = c.get_core_pool_size()
        c.adjust_thread_pool()
        assert c.get_core_pool_size() == before           # no-op

    def test_dec_branch_does_not_change_core_pool_size(self):
        """acc < 0.8 * threshold → Java 调 decCorePoolSize()，同样是空实现。"""
        c = DefaultMQPushConsumer(GROUP)
        c.set_adjust_thread_pool_nums_threshold(1000)
        c._update_msg_acc_cnt("a", [_msg(0, 10)])         # 10 < 800
        before = c.get_core_pool_size()
        c.adjust_thread_pool()
        assert c.get_core_pool_size() == before           # no-op

    def test_explicit_update_still_works_after_adjust(self):
        """自动弹性 no-op ≠ API 失效：显式 update_core_pool_size 仍然生效。"""
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("a", [_msg(0, 10 ** 6)])
        c.adjust_thread_pool()
        assert c.update_core_pool_size(11) is True
        assert c.get_core_pool_size() == 11


class TestRunningInfoReflectsCorePoolSize:
    def test_prop_threadpool_core_size_uses_core_not_max(self):
        c = DefaultMQPushConsumer(GROUP)
        c.name_server_addrs = ["127.0.0.1:9876"]
        c.namespace = None
        c._start_time = time.time()
        c.update_core_pool_size(13)
        info = c.consumer_running_info()
        assert info.properties[ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE] == "13"
        assert info.properties[ConsumerRunningInfo.PROP_CONSUME_TYPE] == "CONSUME_PASSIVELY"

    def test_prop_follows_set_consume_thread_nums(self):
        c = DefaultMQPushConsumer(GROUP)
        c.name_server_addrs = []
        c.namespace = None
        c._start_time = time.time()
        c.set_consume_thread_nums(7)
        info = c.consumer_running_info()
        assert info.properties[ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE] == "7"
