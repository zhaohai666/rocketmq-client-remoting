# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from collections import deque
from types import SimpleNamespace

import pytest

from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullResult, PullStatus
from client.pull_schedule_service import (DEFAULT_PULL_NEXT_DELAY_MILLIS,
                                          PULL_TASK_EXCEPTION_DELAY_MILLIS,
                                          PullConsumerScheduleService,
                                          PullTaskCallback, PullTaskContext)
from common.message import MessageExt, MessageQueue
from remoting.protocol.heartbeat import MessageModel

TOPIC = "TopicSchedUnit"

# 计时断言预算（秒）。Windows 回环端口有 ~2s 拒连延迟，这里虽不连 socket，
# 但线程调度也可能被拖慢，统一给 30s。
WAIT_TIMEOUT = 30.0


def _wait_for(pred, timeout=WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def _msg(topic, broker, qid, offset):
    m = MessageExt(topic, b"x")
    m.broker_name = broker
    m.queue_id = qid
    m.queue_offset = offset
    return m


def _found(offset, step=10):
    """FOUND：next_begin_offset = offset + step，位点推进可被流水验证。"""
    return PullResult(PullStatus.FOUND, offset + step, 0, offset + step,
                      [_msg(TOPIC, "broker-a", 0, offset)])


class FakePullConsumer:
    """DefaultMQPullConsumer 的内存替身：记录每次 pull，队列视图可控。"""

    def __init__(self):
        self.consumer_group = "PG_SchedUnit"
        self.message_model = MessageModel.CLUSTERING
        self.register_topics = set()
        self.started = 0
        self.shutdown_called = 0
        self._assigned = {}
        self._lock = threading.Lock()
        self.pulls = []            # [(mq, offset, sub_expression)]
        self.inflight = set()      # 当前正处于 pull() 里的队列键
        self.serial_violations = []  # 同队列并发进入 pull 的证据（必须恒空）
        self._results = {}         # queue key -> deque（弹空走默认 _found）
        self.committed = {}        # queue key -> int（fetch_consume_offset）
        self.max_offsets = {}      # queue key -> int
        self.pull_hook = None      # 在 pull 内执行的旁路（阻塞/置事件）

    # ---- 测试脚手架 ----
    def set_queues(self, topic, queues):
        with self._lock:
            self._assigned[topic] = list(queues)

    def script(self, mq, results):
        key = (mq.topic, mq.broker_name, mq.queue_id)
        self._results.setdefault(key, deque()).extend(results)

    def counts(self):
        out = {}
        with self._lock:
            for mq, _offset, _sub in self.pulls:
                key = (mq.topic, mq.broker_name, mq.queue_id)
                out[key] = out.get(key, 0) + 1
        return out

    def last_offset(self, mq):
        key = (mq.topic, mq.broker_name, mq.queue_id)
        with self._lock:
            for rec_mq, offset, _sub in reversed(self.pulls):
                if (rec_mq.topic, rec_mq.broker_name, rec_mq.queue_id) == key:
                    return offset
        return None

    # ---- DefaultMQPullConsumer 契约面 ----
    def start(self):
        self.started += 1

    def shutdown(self):
        self.shutdown_called += 1

    def fetch_message_queues_in_balance(self, topic):
        return list(self._assigned.get(topic, []))

    def fetch_subscribe_message_queues(self, topic):
        return list(self._assigned.get(topic, []))

    def fetch_consume_offset(self, mq):
        return self.committed.get((mq.topic, mq.broker_name, mq.queue_id))

    def max_offset(self, mq):
        return self.max_offsets.get((mq.topic, mq.broker_name, mq.queue_id), 100)

    def pull(self, mq, sub_expression="*", offset=0, max_nums=32, timeout_millis=None):
        key = (mq.topic, mq.broker_name, mq.queue_id)
        with self._lock:
            if key in self.inflight:
                self.serial_violations.append(key)
            self.inflight.add(key)
        try:
            if self.pull_hook is not None:
                self.pull_hook(mq)
            q = self._results.get(key)
            if q:
                item = q.popleft()
                if isinstance(item, BaseException):
                    raise item
                if callable(item):
                    item = item(offset)
                result = item
            else:
                result = _found(offset)
            with self._lock:
                self.pulls.append((mq, offset, sub_expression))
            return result
        finally:
            with self._lock:
                self.inflight.discard(key)


class _RecordingCallback(PullTaskCallback):
    def __init__(self, return_delay=None):
        self.lock = threading.Lock()
        self.calls = []
        self.event = threading.Event()
        self.return_delay = return_delay

    def do_pull_task(self, message_queue, pull_result, context):
        with self.lock:
            self.calls.append((message_queue, pull_result, context))
        self.event.set()
        return self.return_delay

    def count(self):
        with self.lock:
            return len(self.calls)


def _queues(n, topic=TOPIC):
    return [MessageQueue(topic, "broker-a", i) for i in range(n)]


def _started_service(queues=None, topic=TOPIC):
    """建服务（复用注入消费者）→ 注册回调 → start，返回 (service, consumer, callback)。"""
    consumer = FakePullConsumer()
    consumer.set_queues(topic, queues if queues is not None else _queues(3))
    service = PullConsumerScheduleService(pull_consumer=consumer)
    callback = _RecordingCallback()
    service.register_pull_task(topic, callback, offset=0, next_delay_millis=10)
    service.start()
    return service, consumer, callback


class TestRegistrationAndExpansion:
    def test_register_requires_topic_and_callback(self):
        consumer = FakePullConsumer()
        service = PullConsumerScheduleService(pull_consumer=consumer)
        with pytest.raises(Exception):
            service.register_pull_task("", _RecordingCallback())
        with pytest.raises(Exception):
            service.register_pull_task(TOPIC, None)

    def test_own_consumer_requires_group(self):
        with pytest.raises(Exception):
            PullConsumerScheduleService()

    def test_register_before_start_only_books_and_marks_heartbeat_topic(self):
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(3))
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback())
        assert TOPIC in consumer.register_topics
        assert service.task_table() == {}
        assert service.callback_topics() == [TOPIC]
        assert consumer.pulls == []

    def test_start_expands_every_assigned_queue(self):
        service, consumer, callback = _started_service(_queues(3))
        try:
            assert _wait_for(lambda: len(consumer.counts()) >= 3), \
                "三个队列都要被拉到"
            assert set(service.task_table()) == set(_queues(3))
            assert callback.count() >= 3
        finally:
            service.shutdown()

    def test_broadcasting_expands_from_subscribe_queues(self):
        consumer = FakePullConsumer()
        consumer.message_model = MessageModel.BROADCASTING
        all_q = _queues(4)
        consumer.set_queues(TOPIC, all_q)
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.counts()) >= 4)
        finally:
            service.shutdown()

    def test_repeated_start_is_noop(self):
        service, consumer, _ = _started_service(_queues(1))
        try:
            service.start()  # 幂等，不重复建线程
            assert len(service.task_table()) == 1
        finally:
            service.shutdown()


class TestDelayAndReschedule:
    def test_not_due_never_dispatches_early(self):
        # 首轮 0ms，第二轮起常驻 30s —— 观察窗口内只应有一轮。
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(1))
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(),
                                   next_delay_millis=30000)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 1)
            time.sleep(0.3)
            assert len(consumer.pulls) == 1, "未到延迟不得再派"
        finally:
            service.shutdown()

    def test_context_delay_resolution_priority(self):
        """显式返回值 > pull_message_later（一次性）> 常驻 set_pull_interval。"""
        ctx = PullTaskContext(FakePullConsumer(), MessageQueue(TOPIC, "broker-a", 0))
        assert ctx.pull_next_delay_millis == DEFAULT_PULL_NEXT_DELAY_MILLIS
        # 1) 返回值压过一切
        ctx.pull_message_later(50)
        assert ctx.consume_next_delay_millis(7) == 7
        # 2) 无返回值时用一次性顺延，且用掉即清
        ctx.pull_message_later(50)
        assert ctx.consume_next_delay_millis(None) == 50
        assert ctx.consume_next_delay_millis(None) == DEFAULT_PULL_NEXT_DELAY_MILLIS
        # 3) 常驻间隔
        ctx.set_pull_interval(300)
        assert ctx.consume_next_delay_millis(None) == 300
        assert ctx.consume_next_delay_millis(None) == 300
        # 4) 负数钳到 0；last_next_delay 可观测
        assert ctx.consume_next_delay_millis(-5) == 0
        assert ctx.last_next_delay_millis == 0

    def test_return_value_reschedules_next_round(self):
        """回调返回下一轮延迟：第一轮返回 0（立刻再来），第二轮返回 30000（不再来）。"""
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(1))

        class _Cw(PullTaskCallback):
            def __init__(self):
                self.n = 0
                self.event = threading.Event()

            def do_pull_task(self, mq, result, context):
                self.n += 1
                if self.n == 2:
                    self.event.set()
                    return 30000
                return 0

        cb = _Cw()
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, cb, next_delay_millis=30000)
        service.start()
        try:
            assert cb.event.wait(WAIT_TIMEOUT), "返回 0 应立刻重排第二轮"
            time.sleep(0.2)
            assert cb.n == 2, "返回 30000 后窗口内不得有第三轮"
        finally:
            service.shutdown()

    def test_pull_message_later_is_one_shot(self):
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(1))
        rounds = []

        class _Cw(PullTaskCallback):
            def do_pull_task(self, mq, result, context):
                context.round_flag = len(rounds)
                if not rounds:
                    # 首轮：一次性顺延 30s（常驻 10ms 被它压过、只此一轮），
                    # 并把堆里的下一次派发等掉 —— 用事件放行替代墙钟。
                    rounds.append(mq)
                    context.pull_message_later(30000)
                    return None
                return None

        cb = _Cw()
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, cb, next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 1)
            time.sleep(0.2)
            assert len(consumer.pulls) == 1, "一次性顺延生效，第二轮被推到 30s 后"
        finally:
            service.shutdown()


class TestOffsetSources:
    def test_fixed_int_offset_then_cursor_follows_next_begin(self):
        service, consumer, cb = _started_service(_queues(1))
        service.get_pull_consumer()  # 覆盖访问器
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 2)
            offs = [p[1] for p in consumer.pulls]
            assert offs[0] == 0
            assert offs[1] == 10, "_found 的 next_begin_offset=offset+10，游标必须跟上"
        finally:
            service.shutdown()

    def test_callable_offset_source_is_evaluated_once(self):
        consumer = FakePullConsumer()
        q0 = MessageQueue(TOPIC, "broker-a", 0)
        consumer.set_queues(TOPIC, [q0])
        calls = []

        def source():
            calls.append(1)
            return 5

        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), offset=source,
                                   next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 2)
            assert consumer.pulls[0][1] == 5
            assert consumer.pulls[1][1] == 15
            assert len(calls) == 1, "来源只求值一次，之后走游标"
        finally:
            service.shutdown()

    def test_none_offset_asks_broker_then_falls_back_to_max(self):
        consumer = FakePullConsumer()
        q0 = MessageQueue(TOPIC, "broker-a", 0)
        q1 = MessageQueue(TOPIC, "broker-a", 1)
        consumer.set_queues(TOPIC, [q0, q1])
        consumer.committed[(TOPIC, "broker-a", 0)] = 7
        consumer.max_offsets[(TOPIC, "broker-a", 1)] = 55
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), offset=None,
                                   next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.counts()) >= 2)
            first = {}
            for mq, off, _s in consumer.pulls:
                first.setdefault(mq.queue_id, off)
            assert first[0] == 7, "有提交位点 → 从位点表续"
            assert first[1] == 55, "无提交位点 → 退队尾（CONSUME_FROM_LAST 语义）"
        finally:
            service.shutdown()

    def test_offset_illegal_adopts_broker_suggested_offset(self):
        consumer = FakePullConsumer()
        q0 = MessageQueue(TOPIC, "broker-a", 0)
        consumer.set_queues(TOPIC, [q0])
        consumer.script(q0, [
            PullResult(PullStatus.OFFSET_ILLEGAL, 42, 40, 90, []),
        ])
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), offset=5,
                                   next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 2)
            assert consumer.pulls[0][1] == 5
            assert consumer.pulls[1][1] == 42, "OFFSET_ILLEGAL 用 next_begin_offset 纠偏"
        finally:
            service.shutdown()

    def test_seek_overrides_cursor(self):
        consumer = FakePullConsumer()
        q0 = MessageQueue(TOPIC, "broker-a", 0)
        consumer.set_queues(TOPIC, [q0])

        class _Cw(PullTaskCallback):
            done = threading.Event()

            def do_pull_task(self, mq, result, context):
                if not self.done.is_set():
                    self.done.set()
                    context.seek(1000)
                    return 0
                return None

        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _Cw(), offset=3, next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 2)
            assert consumer.pulls[0][1] == 3
            assert consumer.pulls[1][1] == 1000, "seek 压过 next_begin_offset"
        finally:
            service.shutdown()


class TestSerialisationAndErrors:
    def test_same_queue_never_pulls_twice_concurrently(self):
        """回调返回 0 让队列尽快连轴转，串行性由 in_flight 闸门结构保证；
        fake 里若同一队列并发进入 pull 会记进 serial_violations（恒空即通过）。"""
        service, consumer, _cb = _started_service(_queues(2))
        try:
            assert _wait_for(lambda: sum(consumer.counts().values()) >= 12)
            assert consumer.serial_violations == []
        finally:
            service.shutdown()

    def test_callback_exception_reschedules_with_1000ms(self):
        pass
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(1))
        second_round = threading.Event()
        release = threading.Event()

        def hook(mq):
            if consumer.pulls:  # 首轮已记账 → 现在进的是第二轮
                second_round.set()
                release.wait(WAIT_TIMEOUT)

        consumer.pull_hook = hook
        class _Cw(PullTaskCallback):
            def do_pull_task(self, mq, result, context):
                raise RuntimeError("boom")

        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _Cw(), next_delay_millis=10)
        service.pull_thread_nums = 2
        service.start()
        try:
            assert second_round.wait(WAIT_TIMEOUT), "异常轮之后必须按 1000ms 重来一轮"
            task = list(service.task_table().values())[0]
            assert task.context.last_next_delay_millis == PULL_TASK_EXCEPTION_DELAY_MILLIS
            assert task.context.pull_next_delay_millis == 10, "常驻间隔不被异常轮污染"
            assert task.context.round_count == 1, "pull 成功即推进游标/轮数，与回调无关"
        finally:
            release.set()
            service.shutdown()

    def test_pull_exception_does_not_stop_schedule(self):
        consumer = FakePullConsumer()
        q0 = MessageQueue(TOPIC, "broker-a", 0)
        consumer.set_queues(TOPIC, [q0])
        # 连失败两轮（失败轮不进 pulls 流水）：第三轮成功时起点仍应是 1 —— 证明
        # 拉取失败既不推进游标、也不让任务从堆里消失。
        consumer.script(q0, [RuntimeError("rpc down"), RuntimeError("rpc down again")])
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), offset=1,
                                   next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 1)
            assert consumer.pulls[0][1] == 1, "失败轮之后的重试仍从原游标起步"
            assert _wait_for(lambda: len(consumer.pulls) >= 2)
            assert consumer.pulls[1][1] == 11, "成功轮才推进游标"
        finally:
            service.shutdown()

    def test_task_without_callback_is_dropped(self):
        service, consumer, _cb = _started_service(_queues(1))
        try:
            assert _wait_for(lambda: len(consumer.pulls) >= 1)
            del service._callback_table[TOPIC]
            assert _wait_for(lambda: service.task_table() == {})
            # 丢任务后不再派发
            n = len(consumer.pulls)
            time.sleep(0.15)
            assert len(consumer.pulls) == n
        finally:
            service.shutdown()


class TestQueueSetChanges:
    def test_revoke_and_add_on_queue_view_change(self):
        consumer = FakePullConsumer()
        q0, q1, q2 = _queues(3)
        consumer.set_queues(TOPIC, [q0, q1])
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.queue_check_interval_millis = 100  # 测试里把复查周期调小
        cb = _RecordingCallback()
        service.register_pull_task(TOPIC, cb, next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.counts()) >= 2)
            consumer.set_queues(TOPIC, [q1, q2])
            assert _wait_for(lambda: set(service.task_table()) == {q1, q2})
            assert _wait_for(lambda: (TOPIC, "broker-a", 2) in consumer.counts())
            revoked_before = consumer.counts().get((TOPIC, "broker-a", 0), 0)
            time.sleep(0.25)
            assert consumer.counts().get((TOPIC, "broker-a", 0), 0) == revoked_before, \
                "撤掉的队列不得再被拉"
        finally:
            service.shutdown()

    def test_queue_view_failure_keeps_current_tasks(self):
        consumer = FakePullConsumer()
        qs = _queues(2)
        consumer.set_queues(TOPIC, qs)
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.queue_check_interval_millis = 100
        service.register_pull_task(TOPIC, _RecordingCallback(), next_delay_millis=10)
        service.start()
        try:
            assert _wait_for(lambda: len(consumer.counts()) >= 2)

            def boom(topic):
                raise RuntimeError("route down")

            consumer.fetch_message_queues_in_balance = boom
            time.sleep(0.35)  # 至少跨两轮复查
            assert set(service.task_table()) == set(qs), "视图拿不到绝不能撤光任务"
        finally:
            service.shutdown()


class TestShutdown:
    def test_shutdown_waits_for_inflight_pull_and_keeps_reused_consumer(self):
        consumer = FakePullConsumer()
        consumer.set_queues(TOPIC, _queues(1))
        entered = threading.Event()
        release = threading.Event()

        def hook(mq):
            if not entered.is_set():
                entered.set()
                release.wait(WAIT_TIMEOUT)

        consumer.pull_hook = hook
        service = PullConsumerScheduleService(pull_consumer=consumer)
        service.register_pull_task(TOPIC, _RecordingCallback(), next_delay_millis=10)
        service.start()
        assert entered.wait(WAIT_TIMEOUT), "第一轮 pull 已进入阻塞"
        done = threading.Event()

        def _shutdown():
            service.shutdown()
            done.set()

        t = threading.Thread(target=_shutdown)
        t.start()
        time.sleep(0.2)
        assert not done.is_set(), "在途 pull 未返回前 shutdown 不得放行（排空）"
        release.set()
        assert done.wait(WAIT_TIMEOUT), "在途 pull 返回后 shutdown 应完成"
        t.join(5.0)
        assert consumer.shutdown_called == 0, "复用的消费者所有权在调用方，服务不得关它"
        assert consumer.serial_violations == []
        service.shutdown()  # 幂等

    def test_own_consumer_is_closed_on_shutdown(self, monkeypatch):
        """consumer_group 构造（服务自建消费者）→ shutdown 先停调度再关消费者，
        消费者又经 MQClientManager 引用计数关实例 —— 用假 MQClientInstance 验证整链。"""
        import client.consumer as consumer_mod

        holder = {}

        q0 = MessageQueue(TOPIC, "broker-a", 0)

        class _FakeClient:
            def __init__(self, *args, **kwargs):
                self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)
                self.shutdown_called = 0
                self.start_called = 0

            def start(self):
                self.start_called += 1

            def shutdown(self):
                self.shutdown_called += 1

            def get_all_broker_addrs(self):
                return []

            def send_heartbeat(self, addr, hb, timeout_millis):
                pass

            def register_topic_in_use(self, topic):
                pass

            def get_topic_publish_info(self, topic):
                return SimpleNamespace(msg_queue_list=[])

            def unregister_client_all_brokers(self, client_id, producer_group,
                                              consumer_group, timeout_millis=3000):
                pass

            def get_topic_subscribe_info(self, topic):
                return [q0] if topic == TOPIC else []

            def get_consumer_id_list_by_group(self, topic, group, timeout_millis=5000):
                # 分配公式要求 currentCID ∈ cidAll：回消费者自己 start 时定出的 clientId。
                return [holder["consumer"].client_id]

            def query_consumer_offset(self, group, mq, timeout_millis=5000):
                return None  # 无提交位点 → offset=None 的来源会落到 max_offset

            def get_max_offset(self, mq, timeout_millis=5000):
                return 100

            def pull_message(self, group, mq, offset, *args, **kwargs):
                return _found(offset)

        def _factory(*args, **kwargs):
            holder["client"] = _FakeClient(*args, **kwargs)
            return holder["client"]

        monkeypatch.setattr(consumer_mod, "MQClientInstance", _factory)
        service = PullConsumerScheduleService(consumer_group="PG_SchedOwn")
        consumer = holder["consumer"] = service.get_pull_consumer()
        consumer.set_namesrv_addr("127.0.0.1:9876")
        consumer.heartbeat_interval_millis = 3600_000
        service.register_pull_task(TOPIC, _RecordingCallback(), next_delay_millis=10)
        service.start()
        assert isinstance(consumer, DefaultMQPullConsumer)
        assert consumer._started is True
        # 队列展开走真 fetch_message_queues_in_balance（读位口径 + 分配公式），
        # 任务键应落到假 client 给出的 q0，并且真的拉到过。
        assert _wait_for(lambda: q0 in service.task_table())
        assert _wait_for(lambda: q0 in consumer._pull_from_which_node)
        client = holder["client"]
        service.shutdown()
        assert consumer._started is False, "自建消费者随服务关闭"
        assert client.shutdown_called == 1, "引用计数归零 → 实例真关"

    def test_operations_after_shutdown_are_noop(self):
        service, consumer, _ = _started_service(_queues(1))
        service.shutdown()
        pulls = len(consumer.pulls)
        time.sleep(0.2)
        assert len(consumer.pulls) == pulls, "停调度后不得再有派发"
        service.shutdown()  # 幂等
