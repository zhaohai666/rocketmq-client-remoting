# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time

import pytest

from client.consume_executor import ConsumeExecutor
from client.consumer import DefaultMQPushConsumer
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from remoting.protocol.body import ConsumerRunningInfo

GROUP = "GID_ThreadPoolUnit"


# ------------------------------------------------------------------ ConsumeExecutor

class TestConsumeExecutor:
    def test_spawns_up_to_core_then_queues(self):
        """core=2: the first two tasks each spawn a thread; the third task only queues, no new thread."""
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
            # Both workers are occupied; the third task can only wait in the queue
            ex.submit(block, "c")
            time.sleep(0.1)
            assert ex.worker_count() == 2
            assert ex.queued_count() == 1
        finally:
            release.set()
            ex.shutdown(wait=True)

    def test_raising_core_spawns_for_queued_tasks(self):
        pass
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
        """Threads above core exit once idle for keep_alive; threads at or below core never exit."""
        ex = ConsumeExecutor(1, 4, keep_alive_seconds=0.2)
        try:
            ex.submit(lambda: None)
            ex.set_core_pool_size(2)          # empty queue → no extra threads; core is now 2
            time.sleep(0.05)
            assert ex.get_core_pool_size() == 2
            # Only 1 worker right now; submit another task to raise it to 2 (still <= core, should survive the timeout)
            ex.submit(lambda: None)
            time.sleep(0.05)
            assert ex.worker_count() == 2
            ex.set_core_pool_size(1)          # core drops to 1 → the extra one becomes "over-provisioned"
            time.sleep(0.6)                   # past keep_alive
            assert ex.worker_count() == 1     # the over-provisioned thread has exited
            time.sleep(0.4)
            assert ex.worker_count() == 1     # threads within core do not exit
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
            assert done.wait(3)               # the same worker can keep working
        finally:
            ex.shutdown(wait=True)

    def test_submit_after_shutdown_raises(self):
        ex = ConsumeExecutor(1, 2)
        ex.shutdown()
        with pytest.raises(RuntimeError):
            ex.submit(lambda: None)

    def test_shutdown_wait_drains_queued_tasks(self):
        pass
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
        """With core=0 (no resident threads), tasks are still executed (threads are spawned on demand at submit time)."""
        ex = ConsumeExecutor(0, 2, keep_alive_seconds=0.1)
        done = threading.Event()
        try:
            ex.submit(done.set)
            assert done.wait(3)
        finally:
            ex.shutdown(wait=True)


# ------------------------------------------------------------------ Consumer side

class TestConsumerDefaultsAndGuards:
    def test_thread_pool_defaults(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c.get_consume_thread_min() == 20
        assert c.get_consume_thread_max() == 20
        assert c.get_adjust_thread_pool_nums_threshold() == 100000
        assert c.get_core_pool_size() == 20              # = consumeThreadMin

    def test_update_core_pool_size_happy_path(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(15) is True
        assert c.get_core_pool_size() == 15

    def test_update_core_pool_size_rejects_zero_and_negative(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(0) is False
        assert c.update_core_pool_size(-1) is False
        assert c.get_core_pool_size() == 20              # not applied

    def test_update_core_pool_size_rejects_above_short_max(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_max(40000)
        assert c.update_core_pool_size(32768) is False
        assert c.update_core_pool_size(32767) is True    # the bound itself is legal
        assert c.get_core_pool_size() == 32767

    def test_update_core_pool_size_rejects_equal_to_consume_thread_max(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        assert c.update_core_pool_size(20) is False      # == consumeThreadMax (5.x default)
        assert c.update_core_pool_size(19) is True
        assert c.get_core_pool_size() == 19

    def test_default_max_is_5x20_not_4x64(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        assert c.get_consume_thread_max() == 20
        assert c.update_core_pool_size(30) is False
        assert c.update_core_pool_size(21) is False
        assert c.get_core_pool_size() == 20              # none of them took effect
        c.set_consume_thread_max(30)
        assert c.update_core_pool_size(25) is True
        assert c.get_core_pool_size() == 25

    def test_set_consume_thread_nums_sets_min_max_and_core(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_nums(4)
        assert c.get_consume_thread_min() == 4
        assert c.get_consume_thread_max() == 4
        assert c.get_core_pool_size() == 4
        # max is now 4 → the guard judges by the new max
        assert c.update_core_pool_size(4) is False
        assert c.update_core_pool_size(3) is True

    def test_consume_thread_min_setter_moves_core(self):
        c = DefaultMQPushConsumer(GROUP)
        c.set_consume_thread_min(8)
        assert c.get_consume_thread_min() == 8
        assert c.get_core_pool_size() == 8
        c.set_consume_thread_max(16)
        assert c.get_consume_thread_max() == 16
        assert c.get_core_pool_size() == 8               # changing max does not move core

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


# ------------------------------------------------------------------ msgAccCnt / threshold

def _msg(queue_offset: int, max_offset=None) -> MessageExt:
    m = MessageExt(topic="T", body=b"x")
    m.queue_offset = queue_offset
    if max_offset is not None:
        m.properties[MessageConst.PROPERTY_MAX_OFFSET] = str(max_offset)
    return m


class TestMsgAccCnt:
    def test_acc_cnt_is_max_offset_minus_queue_offset(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(10, 100), _msg(12, 100)])
        assert c.msg_acc_cnt("k1") == 88                 # 100 - 12

    def test_acc_cnt_ignores_non_positive(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(100, 100)])     # == 0 → not updated
        assert c.msg_acc_cnt("k1") == 0
        c._update_msg_acc_cnt("k1", [_msg(200, 100)])     # < 0 → not updated
        assert c.msg_acc_cnt("k1") == 0

    def test_acc_cnt_missing_property_is_ignored(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("k1", [_msg(10)])           # no MAX_OFFSET property
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
        c._update_msg_acc_cnt("k1", [_msg(400, 450)])     # a new batch with a smaller backlog
        assert c.msg_acc_cnt("k1") == 50

    def test_compute_accumulation_total_sums_all_queues(self):
        c = DefaultMQPushConsumer(GROUP)
        c._update_msg_acc_cnt("a", [_msg(0, 30)])
        c._update_msg_acc_cnt("b", [_msg(0, 70)])
        assert c.compute_accumulation_total() == 100
        assert c.msg_acc_cnt() == 100


class TestAdjustThreadPoolIsNoOp:
    def test_inc_branch_does_not_change_core_pool_size(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        c.set_adjust_thread_pool_nums_threshold(100)
        c._update_msg_acc_cnt("a", [_msg(0, 500)])        # 500 >= 100
        before = c.get_core_pool_size()
        c.adjust_thread_pool()
        assert c.get_core_pool_size() == before           # no-op

    def test_dec_branch_does_not_change_core_pool_size(self):
        pass
        c = DefaultMQPushConsumer(GROUP)
        c.set_adjust_thread_pool_nums_threshold(1000)
        c._update_msg_acc_cnt("a", [_msg(0, 10)])         # 10 < 800
        before = c.get_core_pool_size()
        c.adjust_thread_pool()
        assert c.get_core_pool_size() == before           # no-op

    def test_explicit_update_still_works_after_adjust(self):
        """Auto-scaling being a no-op does not mean the API is dead: an explicit update_core_pool_size still works."""
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
