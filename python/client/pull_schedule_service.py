# -*- coding: utf-8 -*-
from __future__ import annotations

import heapq
import itertools
import threading
import time
from typing import Callable, Dict, List, Optional, Set, Union

from common.message import MessageQueue
from remoting.protocol.heartbeat import MessageModel
from rocketmq_logging import get_logger

from .consume_executor import ConsumeExecutor, RejectedExecutionError
from .consumer import DefaultMQPullConsumer
from .consumer_result import PullResult
from .exception import MQClientException

logger = get_logger()

DEFAULT_PULL_NEXT_DELAY_MILLIS = 200
PULL_TASK_EXCEPTION_DELAY_MILLIS = 1000
DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS = 20000


class PullTaskContext:
    pass

    def __init__(self, pull_consumer, message_queue: MessageQueue,
                 subscription_expression: str = "*",
                 offset: Union[int, Callable[[], int], None] = None,
                 next_delay_millis: int = DEFAULT_PULL_NEXT_DELAY_MILLIS,
                 max_msg_nums: int = 32,
                 timeout_millis: Optional[int] = None):
        self.pull_consumer = pull_consumer
        self.message_queue = message_queue
        self.subscription_expression = subscription_expression
        self.max_msg_nums = max_msg_nums
        self.timeout_millis = timeout_millis
        self.pull_next_delay_millis = int(next_delay_millis)
        self._offset_source = offset
        self.next_offset: Optional[int] = None
        self._one_shot_delay_millis: Optional[int] = None
        # 观测：已完成的拉取轮数 / 上一轮实际采用的延迟（排障与单测都靠它，
        # 不让断言依赖墙钟计时）。
        self.round_count = 0
        self.last_next_delay_millis: Optional[int] = None

    # ---------------- 位点 ----------------
    def resolve_offset(self) -> int:
        """本轮拉取的起始位点：已有游标直接用；首轮按登记的来源求值。"""
        if self.next_offset is None:
            src = self._offset_source
            if src is None:
                self.next_offset = self._offset_from_broker()
            elif callable(src):
                self.next_offset = int(src())
            else:
                self.next_offset = int(src)
        return self.next_offset

    def _offset_from_broker(self) -> int:
        """offset=None 的来源：broker 位点表优先，查不到退到队尾（LAST 语义）。"""
        mq = self.message_queue
        committed = self.pull_consumer.fetch_consume_offset(mq)
        if committed is not None:
            return int(committed)
        return int(self.pull_consumer.max_offset(mq))

    def seek(self, offset: int) -> None:
        """手动改写下一轮起点（回调里根据结果决定重读位置时用）。"""
        self.next_offset = int(offset)

    def record_pull_result(self, result: PullResult) -> None:
        """拉取成功后推进游标（OFFSET_ILLEGAL 时 next_begin_offset 就是 broker 建议位点）。"""
        self.next_offset = int(result.next_begin_offset)
        self.round_count += 1

    # ---------------- 下一轮延迟 ----------------
    def set_pull_interval(self, millis: int) -> None:
        pass
        self.pull_next_delay_millis = int(millis)

    def pull_message_later(self, millis: int) -> None:
        """顺延下一轮 ``millis`` 毫秒（一次性；再下一轮回到常驻间隔）。"""
        self._one_shot_delay_millis = int(millis)

    def consume_next_delay_millis(self, returned_delay: Optional[int]) -> int:
        """结算下一轮延迟：回调显式返回值 > 一次性顺延（消费掉） > 常驻间隔。"""
        if returned_delay is not None:
            delay = int(returned_delay)
        elif self._one_shot_delay_millis is not None:
            delay = self._one_shot_delay_millis
            self._one_shot_delay_millis = None
        else:
            delay = self.pull_next_delay_millis
        delay = max(0, delay)
        self.last_next_delay_millis = delay
        return delay


class PullTaskCallback:
    pass

    def do_pull_task(self, message_queue: MessageQueue, pull_result: PullResult,
                     context: PullTaskContext) -> Optional[int]:
        raise NotImplementedError


class _TaskSpec:
    """register_pull_task 按 topic 存的登记项（回调 + 任务模板参数）。"""

    __slots__ = ("callback", "subscription_expression", "offset",
                 "next_delay_millis", "max_msg_nums", "timeout_millis")

    def __init__(self, callback, subscription_expression, offset,
                 next_delay_millis, max_msg_nums, timeout_millis):
        self.callback = callback
        self.subscription_expression = subscription_expression
        self.offset = offset
        self.next_delay_millis = next_delay_millis
        self.max_msg_nums = max_msg_nums
        self.timeout_millis = timeout_millis

    def new_context(self, pull_consumer, mq: MessageQueue) -> PullTaskContext:
        return PullTaskContext(pull_consumer, mq,
                               subscription_expression=self.subscription_expression,
                               offset=self.offset,
                               next_delay_millis=self.next_delay_millis,
                               max_msg_nums=self.max_msg_nums,
                               timeout_millis=self.timeout_millis)


class _PullTask:
    pass

    __slots__ = ("context", "cancelled", "in_flight")

    def __init__(self, context: PullTaskContext):
        self.context = context
        self.cancelled = False
        # queue 维度串行的闸门：入堆派发前置 True，_run_task 收尾置 False。
        self.in_flight = False


class PullConsumerScheduleService:
    pass

    def __init__(self, consumer_group: Optional[str] = None,
                 pull_consumer: Optional[DefaultMQPullConsumer] = None,
                 rpc_hook=None, namespace: str = "", socks_proxy_config=None):
        if pull_consumer is None:
            if consumer_group is None or not str(consumer_group).strip():
                raise MQClientException(
                    "consumer_group is required when pull_consumer is not given")
            self._pull_consumer = DefaultMQPullConsumer(
                consumer_group, rpc_hook=rpc_hook, namespace=namespace,
                socks_proxy_config=socks_proxy_config)
            self._own_consumer = True
        else:
            self._pull_consumer = pull_consumer
            self._own_consumer = False
        self.pull_thread_nums = 20
        self.queue_check_interval_millis = DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS
        self._callback_table: Dict[str, _TaskSpec] = {}
        self._task_table: Dict[MessageQueue, _PullTask] = {}
        # 到期堆：(monotonic 到期时刻, 入堆序号, task)——序号只为堆比较稳定。
        self._heap: List = []
        self._heap_seq = itertools.count()
        self._cond = threading.Condition()
        self._workers: Optional[ConsumeExecutor] = None
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._next_queue_check_at: Optional[float] = None
        self._started = False

    # ---------------- 对外 API ----------------
    def get_pull_consumer(self) -> DefaultMQPullConsumer:
        pass
        return self._pull_consumer

    def register_pull_task(self, topic: str, callback: PullTaskCallback,
                           sub_expression: str = "*",
                           offset: Union[int, Callable[[], int], None] = None,
                           next_delay_millis: int = DEFAULT_PULL_NEXT_DELAY_MILLIS,
                           max_msg_nums: int = 32,
                           timeout_millis: Optional[int] = None) -> None:
        pass
        if topic is None or not str(topic).strip():
            raise MQClientException("topic is empty")
        if callback is None:
            raise MQClientException("callback is required")
        spec = _TaskSpec(callback, sub_expression, offset, next_delay_millis,
                         max_msg_nums, timeout_millis)
        self._callback_table[topic] = spec
        self._pull_consumer.register_topics.add(topic)
        if self._started:
            self._put_tasks_for_topic(topic)

    def start(self) -> None:
        """拉起消费者（幂等；复用模式下也是——MQClientManager 计数保证安全）、
        建工作池与调度线程，并为所有已登记 topic 展开队列。"""
        if self._started:
            return
        self._pull_consumer.start()
        group = self._pull_consumer.consumer_group
        self._workers = ConsumeExecutor(
            self.pull_thread_nums, self.pull_thread_nums,
            thread_name_prefix="PullMsgThread-%s" % group,
            thread_name_sep="_", thread_index_from=1)
        self._stop.clear()
        with self._cond:
            self._started = True
            self._next_queue_check_at = None
        self._scheduler_thread = threading.Thread(
            target=self._schedule_loop, daemon=True,
            name="rmq-pull-scheduler-%s" % group)
        self._scheduler_thread.start()
        for topic in list(self._callback_table):
            self._put_tasks_for_topic(topic)
        logger.info("PullConsumerScheduleService start OK, group=%s topics=%s",
                    group, sorted(self._callback_table))

    def shutdown(self) -> None:
        pass
        if not self._started:
            return
        self._stop.set()
        with self._cond:
            self._started = False
            for task in list(self._task_table.values()):
                task.cancelled = True
            self._task_table.clear()
            self._heap.clear()
            self._cond.notify_all()
        if self._scheduler_thread is not None:
            self._scheduler_thread.join(timeout=5.0)
            self._scheduler_thread = None
        workers = self._workers
        self._workers = None
        if workers is not None:
            workers.shutdown(wait=True)
        if self._own_consumer:
            self._pull_consumer.shutdown()
        logger.info("PullConsumerScheduleService shutdown OK, group=%s",
                    self._pull_consumer.consumer_group)

    # ---------------- 观测（测试/排障） ----------------
    def task_table(self) -> Dict[MessageQueue, _PullTask]:
        pass
        with self._cond:
            return dict(self._task_table)

    def callback_topics(self) -> List[str]:
        return sorted(self._callback_table)

    def _queues_of_topic(self, topic: str) -> List[MessageQueue]:
        if self._pull_consumer.message_model == MessageModel.BROADCASTING:
            return list(self._pull_consumer.fetch_subscribe_message_queues(topic))
        # CLUSTERING → mqDivided：本实例应得的那份（本端无后台 rebalance，
        # 用带「拿不到视图保留现有分配」兜底的当场分配公式）。
        return list(self._pull_consumer.fetch_message_queues_in_balance(topic))

    def _put_tasks_for_topic(self, topic: str) -> None:
        """把 topic 的队列集合对齐到任务表：撤掉不在新集合的任务、新队列 0 延迟入堆。"""
        spec = self._callback_table.get(topic)
        if spec is None:
            return
        try:
            queues = self._queues_of_topic(topic)
        except Exception as e:  # noqa: BLE001 —— 视图拿不到就保留现有分配，绝不撤光
            logger.debug("queue view unavailable for %s: %s", topic, e)
            return
        new_set = {mq for mq in queues if mq.topic == topic}
        with self._cond:
            if not self._started:
                return
            for mq in list(self._task_table):
                task = self._task_table[mq]
                if mq.topic == topic and mq not in new_set:
                    task.cancelled = True
                    del self._task_table[mq]
            for mq in new_set:
                if mq not in self._task_table:
                    task = _PullTask(spec.new_context(self._pull_consumer, mq))
                    self._task_table[mq] = task
                    heapq.heappush(self._heap, (time.monotonic(),
                                                next(self._heap_seq), task))
            self._cond.notify_all()

    # ---------------- 调度循环 ----------------
    def _schedule_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except BaseException:  # noqa: BLE001 —— 循环体任何异常都不能杀调度线程
                logger.exception("pull schedule loop error")
            with self._cond:
                if self._stop.is_set() or not self._started:
                    break
                timeout = self._sleep_budget_locked()
                self._cond.wait(timeout)
        logger.debug("pull scheduler loop exited")

    def _sleep_budget_locked(self) -> float:
        """持锁状态下算「最多睡多久」：最早到期时刻、队列复查时刻、0.5s 封顶。"""
        deadline = time.monotonic() + 0.5
        if self._heap:
            deadline = min(deadline, self._heap[0][0])
        if self._next_queue_check_at is not None:
            deadline = min(deadline, self._next_queue_check_at)
        return max(0.01, deadline - time.monotonic())

    def _tick(self) -> None:
        """到期任务派发给工作池；到复查时刻则增撤一轮队列。"""
        due: List[_PullTask] = []
        topics_to_check: Optional[List[str]] = None
        now = time.monotonic()
        with self._cond:
            if self._next_queue_check_at is None:
                # 首轮复查从本 tick 起按周期排（start 时已展开过一遍，不必立刻复查）。
                self._next_queue_check_at = now + self.queue_check_interval_millis / 1000.0
            elif now >= self._next_queue_check_at:
                self._next_queue_check_at = now + self.queue_check_interval_millis / 1000.0
                topics_to_check = list(self._callback_table)
            while self._heap and self._heap[0][0] <= now:
                _, _, task = heapq.heappop(self._heap)
                if task.cancelled or task.in_flight:
                    continue
                # 入队前先占住 in_flight：queue 维度串行从这里开始保证。
                task.in_flight = True
                due.append(task)
        if not self._stop.is_set():
            workers = self._workers
            for task in due:
                try:
                    workers.submit(self._run_task, task)
                except (RejectedExecutionError, AttributeError) as e:
                    # 池拒绝（队列满）或已摘除（shutdown 竞窗）：撤闸并顺延重排，
                    # 绝不能让任务从此脱离堆——否则该队列再也不会被拉。
                    task.in_flight = False
                    logger.debug("pull task rejected, reschedule later: %s", e)
                    if isinstance(e, RejectedExecutionError) and not self._stop.is_set():
                        with self._cond:
                            if self._started and not task.cancelled:
                                heapq.heappush(self._heap, (time.monotonic() + 1.0,
                                                            next(self._heap_seq), task))
                                self._cond.notify_all()
        if topics_to_check:
            for topic in topics_to_check:
                self._put_tasks_for_topic(topic)

    # ---------------- 单个任务的一轮（工作线程上跑） ----------------
    def _run_task(self, task: _PullTask) -> None:
        context = task.context
        mq = context.message_queue
        if task.cancelled:
            task.in_flight = False
            return
        result: Optional[PullResult] = None
        returned_delay: Optional[int] = None
        failed = False
        try:
            offset = context.resolve_offset()
            result = self._pull_consumer.pull(
                mq, context.subscription_expression, offset,
                context.max_msg_nums, context.timeout_millis)
        except BaseException as e:  # noqa: BLE001 —— 拉取失败只顺延，不杀线程
            failed = True
            logger.error("pull schedule: pull %s failed: %s", mq, e)
        if task.cancelled:
            task.in_flight = False
            logger.warning("The pull task is cancelled after doPullTask, %s", mq)
            return
        if not failed:
            context.record_pull_result(result)
            spec = self._callback_table.get(mq.topic)
            if spec is None:
                logger.warning("Pull task callback not exist, topic=%s", mq.topic)
                with self._cond:
                    self._task_table.pop(mq, None)
                task.in_flight = False
                return
            try:
                returned_delay = spec.callback.do_pull_task(mq, result, context)
            except BaseException as e:
                logger.error("do_pull_task exception, mq=%s: %s", mq, e)
                returned_delay = PULL_TASK_EXCEPTION_DELAY_MILLIS
        else:
            returned_delay = PULL_TASK_EXCEPTION_DELAY_MILLIS
        delay = context.consume_next_delay_millis(returned_delay)
        task.in_flight = False
        if task.cancelled:
            logger.warning("The pull task is cancelled after doPullTask, %s", mq)
            return
        with self._cond:
            if task.cancelled or not self._started or self._task_table.get(mq) is not task:
                return
            heapq.heappush(self._heap, (time.monotonic() + delay / 1000.0,
                                        next(self._heap_seq), task))
            self._cond.notify_all()


__all__ = [
    "PullConsumerScheduleService",
    "PullTaskCallback",
    "PullTaskContext",
    "DEFAULT_PULL_NEXT_DELAY_MILLIS",
    "PULL_TASK_EXCEPTION_DELAY_MILLIS",
]
