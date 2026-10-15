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
        # observation: number of completed pull rounds / actual delay used in the previous round
        # (both troubleshooting and unit tests rely on it; assertions must not depend on wall-clock timing).
        self.round_count = 0
        self.last_next_delay_millis: Optional[int] = None

    # ---------------- offset ----------------
    def resolve_offset(self) -> int:
        """Starting offset for this pull round: reuse the existing cursor directly; the first round is evaluated from the registered source."""
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
        """Source when offset=None: broker offset table takes priority; fall back to the queue tail (LAST semantics) when not found."""
        mq = self.message_queue
        committed = self.pull_consumer.fetch_consume_offset(mq)
        if committed is not None:
            return int(committed)
        return int(self.pull_consumer.max_offset(mq))

    def seek(self, offset: int) -> None:
        """Manually override the next round's start (used in callbacks to decide the re-read position from the result)."""
        self.next_offset = int(offset)

    def record_pull_result(self, result: PullResult) -> None:
        """Advance the cursor after a successful pull (on OFFSET_ILLEGAL, next_begin_offset is the broker-suggested offset)."""
        self.next_offset = int(result.next_begin_offset)
        self.round_count += 1

    # ---------------- next-round delay ----------------
    def set_pull_interval(self, millis: int) -> None:
        pass
        self.pull_next_delay_millis = int(millis)

    def pull_message_later(self, millis: int) -> None:
        """Defer the next round by ``millis`` ms (one-shot; the round after returns to the resident interval)."""
        self._one_shot_delay_millis = int(millis)

    def consume_next_delay_millis(self, returned_delay: Optional[int]) -> int:
        """Settle the next-round delay: explicit callback return value > one-shot deferral (consumed) > resident interval."""
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
    """Registration entry stored per topic by register_pull_task (callback + task template params)."""

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
        # gate for queue-dimensional serialization: set True before heap dispatch, set False at the end of _run_task.
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
        # due heap: (monotonic due time, enqueue sequence, task) -- the sequence only keeps heap comparison stable.
        self._heap: List = []
        self._heap_seq = itertools.count()
        self._cond = threading.Condition()
        self._workers: Optional[ConsumeExecutor] = None
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._next_queue_check_at: Optional[float] = None
        self._started = False

    # ---------------- public API ----------------
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
        """Start the consumer (idempotent; also under reuse mode -- MQClientManager's count guarantees
        safety), build the worker pool and scheduling thread, and expand queues for all registered topics."""
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

    # ---------------- observation (testing/troubleshooting) ----------------
    def task_table(self) -> Dict[MessageQueue, _PullTask]:
        pass
        with self._cond:
            return dict(self._task_table)

    def callback_topics(self) -> List[str]:
        return sorted(self._callback_table)

    def _queues_of_topic(self, topic: str) -> List[MessageQueue]:
        if self._pull_consumer.message_model == MessageModel.BROADCASTING:
            return list(self._pull_consumer.fetch_subscribe_message_queues(topic))
        # CLUSTERING -> mqDivided: the share this instance should get (no background rebalance on this side,
        # uses an on-the-spot allocation formula with a "keep existing allocation if the view is unavailable" fallback).
        return list(self._pull_consumer.fetch_message_queues_in_balance(topic))

    def _put_tasks_for_topic(self, topic: str) -> None:
        """Align the topic's queue set to the task table: drop tasks not in the new set, enqueue new queues with 0 delay."""
        spec = self._callback_table.get(topic)
        if spec is None:
            return
        try:
            queues = self._queues_of_topic(topic)
        except Exception as e:  # noqa: BLE001 -- if the view is unavailable, keep the existing allocation; never drop everything
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

    # ---------------- scheduling loop ----------------
    def _schedule_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except BaseException:  # noqa: BLE001 -- no exception in the loop body may kill the scheduling thread
                logger.exception("pull schedule loop error")
            with self._cond:
                if self._stop.is_set() or not self._started:
                    break
                timeout = self._sleep_budget_locked()
                self._cond.wait(timeout)
        logger.debug("pull scheduler loop exited")

    def _sleep_budget_locked(self) -> float:
        """Under the lock, compute "sleep at most how long": earliest due time, queue recheck time, capped at 0.5s."""
        deadline = time.monotonic() + 0.5
        if self._heap:
            deadline = min(deadline, self._heap[0][0])
        if self._next_queue_check_at is not None:
            deadline = min(deadline, self._next_queue_check_at)
        return max(0.01, deadline - time.monotonic())

    def _tick(self) -> None:
        """Dispatch due tasks to the worker pool; when the recheck time arrives, add/drop one round of queues."""
        due: List[_PullTask] = []
        topics_to_check: Optional[List[str]] = None
        now = time.monotonic()
        with self._cond:
            if self._next_queue_check_at is None:
                # the first recheck is scheduled from this tick by period (already expanded once at start, no immediate recheck needed).
                self._next_queue_check_at = now + self.queue_check_interval_millis / 1000.0
            elif now >= self._next_queue_check_at:
                self._next_queue_check_at = now + self.queue_check_interval_millis / 1000.0
                topics_to_check = list(self._callback_table)
            while self._heap and self._heap[0][0] <= now:
                _, _, task = heapq.heappop(self._heap)
                if task.cancelled or task.in_flight:
                    continue
                # occupy in_flight before enqueue: queue-dimensional serialization is guaranteed from here.
                task.in_flight = True
                due.append(task)
        if not self._stop.is_set():
            workers = self._workers
            for task in due:
                try:
                    workers.submit(self._run_task, task)
                except (RejectedExecutionError, AttributeError) as e:
                    # pool rejected (queue full) or already removed (shutdown race window): release the gate
                    # and re-enqueue with deferral; never let the task leave the heap -- otherwise that queue
                    # would never be pulled again.
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

    # ---------------- one round of a single task (runs on a worker thread) ----------------
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
        except BaseException as e:  # noqa: BLE001 -- a failed pull only defers; does not kill the thread
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
