# -*- coding: utf-8 -*-
"""MQPullConsumerScheduleService 等价物（对应 Java
``org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService``，
并结合 ``PullMessageService`` 的「到期取任务 → 执行 → 按回调决定下一轮延迟」调度形态）。

Java 原型的分工：
* ``MQPullConsumerScheduleService.registerPullTaskCallback(topic, callback)`` 按 topic
  登记回调；rebalance 回调（``MessageQueueListenerImpl.messageQueueChanged``）把本实例
  应得的队列展开成 ``taskTable``（queue → PullTaskImpl），经 ``putTask`` 增撤；
* 每个 ``PullTaskImpl`` 在 ``ScheduledThreadPoolExecutor``（``pullThreadNums``，默认 20）
  上运行：调 ``callback.doPullTask(mq, context)``，回调异常兜 1000ms，随后按
  ``context.pullNextDelayTimeMillis`` 重排自己；
* ``shutdown()`` 先关调度池、再关内部 consumer。

本端与 Java 的两处**有意差别**（调度模型按任务规格收敛）：

1. **拉取由服务发起，回调收 PullResult**。Java 里回调自己调
   ``context.getPullConsumer().pullAsync(...)`` 再手动 ``pullMessageLater``；本端把
   「pull_sync → 投递结果 → 按回调决定下一轮延迟」收进调度框架：服务按延迟到期取任务
   → 调 ``PullConsumer.pull`` → 把 ``PullResult`` 交给 ``do_pull_task`` → 回调**返回
   next_delay**（毫秒），或调用 ``context.set_pull_interval``（常驻间隔）/
   ``context.pull_message_later``（一次性顺延）决定下一轮。优先级：显式返回值 >
   一次性顺延 > 常驻间隔。
2. **queue 维度串行**：同一 queue 同时只有一个在途 pull（``in_flight`` 闸门），上一轮
   完成（含回调）才可能重排下一轮——Java 的原实现依赖回调自觉，本端直接结构性保证。

队列展开的口径对齐 Java ``putTask``：topic 的队列集合变化时，撤销「不在新集合里」的
任务（``cancelled`` 后不再重排），补上「新出现」的队列（0 延迟立即首拉）。队列视图的
来源：CLUSTERING 用 ``fetch_message_queues_in_balance``（本实例应得的那份，本端公式与
Java rebalanceByTopic 同源且带「路由拿不到保留现有分配」的兜底）；BROADCASTING 用
``fetch_subscribe_message_queues``（全量）。

consumer 生命周期（对齐「shutdown 时先停调度再关 consumer，或复用调用方 consumer」）：

* ``PullConsumerScheduleService(consumer_group=...)``：服务自建 ``DefaultMQPullConsumer``
  （等价 Java 内部 new DefaultMQPullConsumer），``start()`` 拉起、``shutdown()`` 关掉；
* ``PullConsumerScheduleService(pull_consumer=已有实例)``：复用调用方的消费者——
  本端消费者按 clientId 走 ``MQClientManager`` 引用计数，服务 ``start()`` 幂等地拉起它，
  ``shutdown()`` **不**关它（所有权在调用方）。

命名遵循本端 snake_case 风格；线程名与 Java 口径一致（``PullMsgThread_<group>_<N>``，
即 ``ThreadFactoryImpl("PullMsgThread-" + group)`` 的产物）。
"""
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

# Java PullTaskContext.pullNextDelayTimeMillis 的默认值（200ms）。
DEFAULT_PULL_NEXT_DELAY_MILLIS = 200
# Java doPullTask 抛异常后的下一轮延迟（PullTaskImpl.run 的 catch 分支写 1000ms）。
PULL_TASK_EXCEPTION_DELAY_MILLIS = 1000
# Java DefaultMQPullConsumerImpl 的 rebalancePullInterval（队列视图复查周期）。
DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS = 20000


class PullTaskContext:
    """单个 queue 的拉取任务上下文（对应 Java ``PullTaskContext``，并按任务规格扩展：
    订阅表达式、消费位点来源、下次拉取延迟都挂在它上面）。

    * ``subscription_expression``：本轮之后每轮 pull 用的 tag 表达式（回调里改它，
      下一轮生效——pull 发生在回调之前）。
    * 位点游标 ``next_offset``：首轮拉取的起点由 ``offset`` 决定——
      int（固定起点）、callable() → int（惰性求值，例如包 ``fetch_consume_offset``）、
      None（服务首轮向 broker 问位点：``fetch_consume_offset``，查不到退到 ``max_offset``
      ——CONSUME_FROM_LAST 语义）。此后每轮自动跟随 ``PullResult.next_begin_offset``
      （OFFSET_ILLEGAL 时 broker 就在该字段里给建议位点），也可 ``seek()`` 手动改写。
    * ``pull_next_delay_millis``：常驻的「下次拉取延迟」（Java 同名语义，默认 200ms），
      ``set_pull_interval`` 改它。
    * ``pull_message_later(millis)``：一次性顺延下一轮（用完即清，对齐本端推送消费者
      suspend 场景里「只顺延一轮」的直觉），优先级低于回调显式返回值、高于常驻值。
    """

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
        """设置常驻的下次拉取延迟（Java ``setPullNextDelayTimeMillis`` 同义）。"""
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
    """queue 维度的拉取任务回调（对应 Java ``PullTaskCallback``）。

    与 Java 的区别（见模块文档第 1 点）：服务已经替你把 ``pull_sync`` 做完了，
    这里只处理结果。**返回值**决定下一轮延迟（毫秒，None 走 context 的规则）。
    抛异常不会打断调度：本轮按 1000ms 顺延（Java PullTaskImpl 的 catch 分支）。
    """

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
    """queue 维度的任务记录（对应 Java PullTaskImpl）。"""

    __slots__ = ("context", "cancelled", "in_flight")

    def __init__(self, context: PullTaskContext):
        self.context = context
        self.cancelled = False
        # queue 维度串行的闸门：入堆派发前置 True，_run_task 收尾置 False。
        self.in_flight = False


class PullConsumerScheduleService:
    """按队列调度拉取、把结果交给回调并按回调指示重排下一轮的高层消费者服务。

    用法（与 Java MQPullConsumerScheduleService 对照）::

        service = PullConsumerScheduleService(consumer_group="PG_demo")
        service.register_pull_task("TopicA", MyCallback(), sub_expression="TagA")
        service.start()          # 自建/复用 PullConsumer 并拉起，展开队列，起调度线程
        ...
        service.shutdown()       # 先停调度（排空在途），再关自建 consumer（复用时不关）
    """

    def __init__(self, consumer_group: Optional[str] = None,
                 pull_consumer: Optional[DefaultMQPullConsumer] = None,
                 rpc_hook=None, namespace: str = "", socks_proxy_config=None):
        if pull_consumer is None:
            if consumer_group is None or not str(consumer_group).strip():
                raise MQClientException(
                    "consumer_group is required when pull_consumer is not given")
            # 等价 Java：内部 new DefaultMQPullConsumer(group)，CLUSTERING 默认口径。
            self._pull_consumer = DefaultMQPullConsumer(
                consumer_group, rpc_hook=rpc_hook, namespace=namespace,
                socks_proxy_config=socks_proxy_config)
            self._own_consumer = True
        else:
            self._pull_consumer = pull_consumer
            self._own_consumer = False
        # Java pullThreadNums 默认 20（ScheduledThreadPoolExecutor 的池大小）。
        self.pull_thread_nums = 20
        # 队列视图复查周期（Java rebalancePullInterval 的角色；测试里可调小）。
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
        """底层拉取消费者（对应 Java getDefaultMQPullConsumer()）。"""
        return self._pull_consumer

    def register_pull_task(self, topic: str, callback: PullTaskCallback,
                           sub_expression: str = "*",
                           offset: Union[int, Callable[[], int], None] = None,
                           next_delay_millis: int = DEFAULT_PULL_NEXT_DELAY_MILLIS,
                           max_msg_nums: int = 32,
                           timeout_millis: Optional[int] = None) -> None:
        """按 topic 登记回调；已启动时立即为本实例应得队列展开任务（0 延迟首拉）。

        对应 Java ``registerPullTaskCallback(topic, callback)``（那边回调自己发起
        pull；本端把 pull 收进服务，见模块文档）+ ``MessageQueueListenerImpl`` 的
        队列展开。Java 还会 ``registerMessageQueueListener(topic, null)`` 把 topic 记
        进 registerTopics（心跳订阅集 / 路由刷新的来源），这里同样登记进去。

        ``offset`` 是消费位点来源：int / callable()→int / None（首轮问 broker 位点表，
        查不到退到队尾）。``next_delay_millis`` 是常驻的下次拉取延迟，回调可在
        ``do_pull_task`` 里返回值或用 context 方法逐轮改写。
        """
        if topic is None or not str(topic).strip():
            raise MQClientException("topic is empty")
        if callback is None:
            raise MQClientException("callback is required")
        spec = _TaskSpec(callback, sub_expression, offset, next_delay_millis,
                         max_msg_nums, timeout_millis)
        self._callback_table[topic] = spec
        self._pull_consumer.register_topics.add(topic)
        if self._started:
            # Java 是等下一次 rebalance 才展开；本端队列视图要问消费者（没有推模式
            # 的后台 rebalance 回调可挂），所以调用方线程当场展开一次，之后由
            # 周期复查负责增撤。
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
        """先停调度（撤任务 + 停线程 + **排空在途 pull**），再按所有权决定是否
        关闭消费者（自建 → 关；调用方复用 → 不关，Java 无条件关内部 consumer，
        这里唯一的差别就是复用场景的所有权）。幂等。"""
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
            # 排空：已派发/排队的任务跑完（撤掉的在 _run_task 入口秒回），
            # Java shutdown 调度池后靠 pull 自身超时约束排空时长，这里 join 等待。
            workers.shutdown(wait=True)
        if self._own_consumer:
            self._pull_consumer.shutdown()
        logger.info("PullConsumerScheduleService shutdown OK, group=%s",
                    self._pull_consumer.consumer_group)

    # ---------------- 观测（测试/排障） ----------------
    def task_table(self) -> Dict[MessageQueue, _PullTask]:
        """当前任务表快照（queue → 任务），对应 Java getTaskTable。"""
        with self._cond:
            return dict(self._task_table)

    def callback_topics(self) -> List[str]:
        return sorted(self._callback_table)

    # ---------------- 队列展开（Java putTask 语义） ----------------
    def _queues_of_topic(self, topic: str) -> List[MessageQueue]:
        if self._pull_consumer.message_model == MessageModel.BROADCASTING:
            # Java MessageQueueListenerImpl：BROADCASTING → mqAll（全量订阅信息）。
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
                    # Java putTask：不在新集合 → setCancelled(true) + remove。
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
            # 在途期间被撤（shutdown / 队列变更）：不再重排（Java 同款告警语义）。
            task.in_flight = False
            logger.warning("The pull task is cancelled after doPullTask, %s", mq)
            return
        if not failed:
            context.record_pull_result(result)
            spec = self._callback_table.get(mq.topic)
            if spec is None:
                # Java run() 的「回调不存在」分支：丢任务不重排。
                logger.warning("Pull task callback not exist, topic=%s", mq.topic)
                with self._cond:
                    self._task_table.pop(mq, None)
                task.in_flight = False
                return
            try:
                returned_delay = spec.callback.do_pull_task(mq, result, context)
            except BaseException as e:  # noqa: BLE001 —— Java catch(Throwable)→1000ms
                logger.error("do_pull_task exception, mq=%s: %s", mq, e)
                returned_delay = PULL_TASK_EXCEPTION_DELAY_MILLIS
        else:
            # 与 Java 的异常轮一致：兜 1000ms 再试（只影响本轮，不覆盖常驻间隔）。
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
