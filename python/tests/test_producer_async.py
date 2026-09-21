# -*- coding: utf-8 -*-
"""异步发送链（对应 Java DefaultMQProducerImpl.send(msg, SendCallback, timeout) +
MQClientAPIImpl.sendMessageAsync / onExceptionImpl）的单测。

对齐基准是 Java 5.5.1，盯的是这些真实行为（不是"异步 = 开个线程"）：

  * ``send_async`` **不阻塞调用方**：准备工作全部在 ``AsyncSenderExecutor_N`` 里做
    （池大小 = CPU 核数、队列有界 50000），队列满了才把 ``executor rejected`` 抛给调用方；
  * 出队之后才算耗时：预算被排队吃掉直接回调
    ``RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")``，一次请求都不发；
  * ASYNC 在 ``sendDefaultImpl`` 里 ``timesTotal`` 固定为 1 —— 外层循环不换 broker，
    换 broker 只发生在 ``onExceptionImpl``，上限是 ``retryTimesWhenSendAsyncFailed``；
  * 重试**复用同一个 RemotingCommand**（请求只建一次）并换新 opaque —— 旧 opaque 还挂在
    responseTable 里等超时，复用会让两次尝试的应答串台；
  * 超时预算是**共享**的剩余时间（``timeoutMillis - cost``），不是每次尝试各给一份；
  * 只有 remoting 层的异常才重试：``RemotingTimeoutException`` / ``RemotingSendRequestException``
    包成 ``MQClientException("wait response timeout, cost=…")`` / ``("send request failed")``，
    其余 ``RemotingException`` 包成 ``("unknown reason")`` 且 ``RemotingTooMuchRequestException``
    不重试；
  * ⚠ **已经收到响应**但 broker 回了错误码（``MQBrokerException``）**不重试、也不包装** ——
    异步发送不看 ``retryResponseCodes``，这点和同步发送完全不同；
  * 定点发送（带 mq）时 Java 传的 topicPublishInfo 是 null，所以重试**回到同一台 broker**；
  * 用户回调与 ``SendMessageHook.after`` 在 ``NettyClientPublicExecutor_N`` 上跑（Java 的
    ``executeInvokeCallback`` 把回调 submit 到 publicExecutor，池不可用才就地跑），
    用户回调抛异常被吞掉，不能带走 worker。

真机那段（消息真的落到 broker、回调里拿到 SEND_OK）在 ``verify_message_types.py`` 第 1 节。
"""
from __future__ import annotations

import threading
import time
from typing import List, Optional

import pytest

from rocketmq.client import producer as producer_module
from rocketmq.client.consume_executor import ConsumeExecutor
from rocketmq.client.exception import (ClientErrorCode, MQBrokerException,
                                       MQClientException)
from rocketmq.client.hook import CommunicationMode, SendMessageContext
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.exception import (RemotingConnectException,
                                         RemotingSendRequestException,
                                         RemotingTimeoutException,
                                         RemotingTooMuchRequestException)
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
ADDR_COLD = "127.0.0.1:10913"


def _mq(broker: str, queue_id: int = 0) -> MessageQueue:
    return MessageQueue("TopicTest", broker, queue_id)


class _Deliver:
    """fake 用**回调**投递的结果（区别于就地抛出的异常）。"""

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error


class _Callback:
    """记录回调的测试替身。"""

    def __init__(self, raise_error: bool = False):
        self.results: List[SendResult] = []
        self.errors: List[BaseException] = []
        self.threads: List[str] = []
        self.done = threading.Event()
        self._raise_error = raise_error

    def on_success(self, send_result: SendResult) -> None:
        self.threads.append(threading.current_thread().name)
        self.results.append(send_result)
        if self._raise_error:
            raise RuntimeError("boom in onSuccess")
        self.done.set()

    def on_exception(self, e: BaseException) -> None:
        self.threads.append(threading.current_thread().name)
        self.errors.append(e)
        if self._raise_error:
            raise RuntimeError("boom in onException")
        self.done.set()


class _FakeAsyncClient:
    """MQClientInstance 替身，缝在异步发送用到的那三个接口上。

    ``outcomes`` 每一项：BaseException -> 从 ``send_message_async`` **就地抛出**（对应 Java
    ``sendMessageAsync`` 的 try-catch）；``_Deliver`` -> 走 ``on_complete`` 回调。
    列表用完之后一律回 SEND_OK。
    """

    def __init__(self, queues: List[MessageQueue], outcomes: Optional[list] = None,
                 no_route: bool = False, block: Optional[threading.Event] = None,
                 route=None, cold_brokers=()):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = list(queues)
        self.outcomes = list(outcomes or [])
        self.no_route = no_route
        self.block = block
        # 路由表里查不到发布地址的 broker（对应 Java findBrokerAddressInPublish 返回 null）
        self.cold_brokers = set(cold_brokers)
        # get_topic_route_data 的返回值（Java sendKernelImpl 里那次按 topic 刷路由）
        self.route = route
        self.attempts: List[tuple] = []   # (addr, broker_name, opaque, timeout)
        self.built: List[int] = []        # build_send_request 收到的 mq 个数（用于计数）
        self.build_threads: List[str] = []   # 建请求时所在的线程名
        self.sleep_ms = 0                 # 每次尝试耗时，用来验证"共享超时预算"
        self.lock = threading.Lock()

    # --- 生产者异步链会碰到的 MQClientInstance 接口 ---
    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        if self.no_route:
            raise MQClientException("No route info of this topic: %s" % topic)
        if not self.publish.ok():
            raise MQClientException("Can not find Message Queue for topic: %s" % topic)
        return self.publish

    def broker_addr_of(self, broker_name):
        if broker_name in self.cold_brokers:
            return None
        if broker_name == "broker-a":
            return ADDR_A
        if broker_name == "broker-b":
            return ADDR_B
        raise MQClientException("no broker service info for %s" % broker_name)

    def get_topic_route_data(self, topic):
        return self.route

    def build_send_request(self, group, msg, mq, timeout, sys_flag, unit_mode=False):
        with self.lock:
            self.built.append(mq.broker_name)
            # 建请求发生在 AsyncSenderExecutor 线程上 —— 顺手记下线程名供断言用
            self.build_threads.append(threading.current_thread().name)
        return RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, None)

    def send_message_async(self, addr, request, msg, mq, timeout, on_complete):
        with self.lock:
            self.attempts.append((addr, mq.broker_name, request.opaque, timeout))
        if self.block is not None:
            self.block.wait(5.0)
        if self.sleep_ms:
            time.sleep(self.sleep_ms / 1000.0)
        outcome = None
        if self.outcomes:
            outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, _Deliver):
            on_complete(outcome.result, outcome.error)
            return
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    def shutdown(self):
        pass

    # --- 断言用 ---
    @property
    def attempts_count(self) -> int:
        return len(self.attempts)

    @property
    def brokers(self) -> List[str]:
        return [broker for _, broker, _, _ in self.attempts]

    @property
    def opques(self) -> List[int]:
        return [opaque for _, _, opaque, _ in self.attempts]

    @property
    def timeouts(self) -> List[int]:
        return [timeout for _, _, _, timeout in self.attempts]


class _RecordingHook:
    """SendMessageHook 替身：记录 before/after 各跑了几次、after 带了什么。"""

    def __init__(self):
        self.before: List[SendMessageContext] = []
        self.after: List[SendMessageContext] = []

    def hook_name(self):
        return "recording"

    def send_message_before(self, context: SendMessageContext) -> None:
        self.before.append(context)

    def send_message_after(self, context: SendMessageContext) -> None:
        self.after.append(context)


def _producer(queues=None, outcomes=None, async_retry=2, no_route=False,
              block=None, queue_capacity=50000, route=None, cold_brokers=()) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_async_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(2)
    p.retry_times_when_send_async_failed = async_retry
    p.async_sender_queue_capacity = queue_capacity
    # start() 会建真实客户端，这里只补齐异步链需要的三样：started 标记 + 两个池
    p._create_async_executors()
    p._mq_client = _FakeAsyncClient(
        [_mq("broker-a", 0), _mq("broker-b", 1)] if queues is None else list(queues),
        outcomes, no_route, block, route, cold_brokers)
    p._started = True
    return p


def _client(p: DefaultMQProducer) -> _FakeAsyncClient:
    return p._mq_client


def _thread_names(pool: ConsumeExecutor) -> List[str]:
    """池子里当前 worker 的线程名（``ThreadFactoryImpl`` 的产物）。"""
    return [t.name for t in pool._threads]


def _wait_done(cb: _Callback, timeout: float = 5.0) -> None:
    assert cb.done.wait(timeout), "异步回调没有触发"


def _wait_attempts(client: _FakeAsyncClient, n: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while client.attempts_count < n and time.monotonic() < deadline:
        time.sleep(0.005)


# ---------------------------------------------------------------- 调用方不阻塞
def test_send_async_returns_before_the_request_is_issued():
    """Java：任务只是 submit 到 asyncSenderExecutor，调用方立刻返回。"""
    gate = threading.Event()
    p = _producer(block=gate)
    cb = _Callback()
    t0 = time.monotonic()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    assert elapsed_ms < 500, "send_async 阻塞了调用方 %dms" % elapsed_ms
    assert not cb.done.is_set()
    gate.set()
    _wait_done(cb)
    assert len(cb.results) == 1


def test_send_work_happens_off_the_caller_thread():
    p = _producer()
    seen = {}

    class _Spy:
        def on_success(self, result):
            seen["callback_thread"] = threading.current_thread().name
            seen["result"] = result

        def on_exception(self, e):
            seen["callback_thread"] = threading.current_thread().name
            seen["error"] = e

    caller = threading.current_thread().name
    p.send_async(Message("TopicTest", b"x"), _Spy(), 3000)
    for _ in range(1000):
        if "callback_thread" in seen:
            break
        time.sleep(0.005)
    assert "result" in seen, seen
    # Java 的 ThreadFactoryImpl 名字形如 AsyncSenderExecutor_1 / NettyClientPublicExecutor_1
    assert seen["callback_thread"].startswith("NettyClientPublicExecutor_")
    assert seen["callback_thread"] != caller
    assert _client(p).attempts_count == 1


# ---------------------------------------------------------------- 成功路径
def test_async_success_callback_and_hook_after():
    p = _producer()
    hook = _RecordingHook()
    p.register_send_message_hook(hook)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert len(cb.results) == 1 and cb.results[0].send_status == SendStatus.SEND_OK
    assert not cb.errors
    assert len(hook.before) == 1 and len(hook.after) == 1
    # Java sendKernelImpl 的 ASYNC 分支：communicationMode 要标成 ASYNC
    assert hook.before[0].communication_mode == CommunicationMode.ASYNC
    assert hook.after[0].send_result is cb.results[0]
    assert hook.after[0].exception is None


def test_user_callback_exception_does_not_kill_the_pool():
    """Java：onSuccess/onException 外面包着 catch(Throwable)，回调异常不能带走 worker。"""
    p = _producer()
    first = _Callback(raise_error=True)
    p.send_async(Message("TopicTest", b"x"), first, 3000)
    for _ in range(1000):
        if first.threads:
            break
        time.sleep(0.005)
    assert first.threads and first.results
    second = _Callback()
    p.send_async(Message("TopicTest", b"y"), second, 3000)
    _wait_done(second)
    assert second.results


# ---------------------------------------------------------------- 排队与耗时
def test_zero_timeout_fails_without_sending_anything():
    """Java sendDefaultImpl 的 runnable：timeout <= costTime 直接 onException。"""
    p = _producer()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 0)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "DEFAULT ASYNC send call timeout" in str(cb.errors[0])


def test_bounded_queue_rejects_the_caller_not_the_callback():
    """Java：executor.submit 抛 RejectedExecutionException → MQClientException("executor rejected")。

    这里把池压成 1 个 worker + 1 个队列位（真实池是 CPU 核数 + 50000），
    好让"占住 worker → 填满队列 → 第三个被拒"这条链跑得快且确定。
    """
    gate = threading.Event()
    p = _producer(block=gate)
    p._async_sender_executor = ConsumeExecutor(1, 1, thread_name_prefix="AsyncSenderExecutor",
                                              max_queue_size=1, thread_name_sep="_",
                                              thread_index_from=1)
    running = _Callback()
    p.send_async(Message("TopicTest", b"1"), running, 3000)
    _wait_attempts(_client(p), 1)          # 第一个任务已占住唯一的 worker
    queued = _Callback()
    p.send_async(Message("TopicTest", b"2"), queued, 3000)   # 进队列
    for _ in range(200):
        if p._async_sender_executor.queued_count() == 1:
            break
        time.sleep(0.005)
    assert p._async_sender_executor.queued_count() == 1
    with pytest.raises(MQClientException) as ei:
        p.send_async(Message("TopicTest", b"3"), _Callback(), 3000)
    assert "executor rejected" in str(ei.value)
    gate.set()
    _wait_done(running)
    _wait_done(queued)


def test_not_started_producer_raises_synchronously():
    p = DefaultMQProducer("GID_async_test")
    with pytest.raises(MQClientException):
        p.send_async(Message("TopicTest", b"x"), _Callback(), 3000)


# ---------------------------------------------------------------- 换 broker 重试
def test_connect_failure_retries_with_new_opaque_and_shrinking_budget():
    """Java onExceptionImpl：times<=retryTimesWhenSendAsyncFailed 才重试，
    每轮换 broker、换 opaque、复用同一个请求，超时用共享的剩余预算。"""
    outcomes = [RemotingConnectException(ADDR_A), RemotingConnectException(ADDR_B),
                RemotingConnectException(ADDR_A)]
    p = _producer(outcomes=outcomes, async_retry=2)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    client = _client(p)
    assert client.attempts_count == 3          # 1 次原始 + 2 次重试
    assert len(client.built) == 1              # 请求只建一次，跨重试复用
    assert len(set(client.opques)) == 3        # 每次尝试一个新 opaque
    assert client.brokers[0] != client.brokers[1]   # 避开刚失败的那台
    assert client.timeouts[1] <= client.timeouts[0]
    assert len(cb.results) == 0 and len(cb.errors) == 1
    # Java sendMessageAsync 的**外层 catch**：异常原样交给回调（不包 MQClientException），
    # 并且 needRetry=true。包装成 "send request failed"/"unknown reason" 的是
    # operationFail 那条路（见下面两个用例）。
    assert isinstance(cb.errors[0], RemotingConnectException)


def test_retry_cap_zero_disables_failover():
    p = _producer(outcomes=[_Deliver(error=RemotingTimeoutException(ADDR_A, 3000))],
                  async_retry=0)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    # 上限 0 次重试：只发一次就回调
    assert _client(p).attempts_count == 1
    assert "wait response timeout" in str(cb.errors[0])


def test_broker_error_code_is_returned_as_is_without_retry():
    """⚠ 与同步发送不同：响应已收到 → needRetry=false，异常也**不包装**。

    Java ``sendMessageAsync`` 的 ``operationSucceed`` 里 ``processSendResponse`` 抛
    ``MQBrokerException`` 会进 ``catch (Exception e)`` → ``onExceptionImpl(..., false, ...)``。
    """
    broker_error = MQBrokerException(ResponseCode.SYSTEM_BUSY, "broker busy")
    p = _producer(outcomes=[_Deliver(error=broker_error)], async_retry=2)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 1
    assert cb.errors[0] is broker_error


def test_retry_cap_is_read_from_config():
    """retry_times_when_send_async_failed 过去只是个存下来没人读的字段。"""
    p = _producer(outcomes=[RemotingConnectException(ADDR_A)] * 5, async_retry=4)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 5      # 1 + 4


def test_retry_budget_is_shared_not_per_attempt():
    """Java onExceptionImpl 传的是 ``timeoutMillis - cost``：预算**共享**，
    第二次尝试拿到的超时是剩下的那一点，而不是又一个完整 timeout。"""
    p = _producer(outcomes=[RemotingConnectException(ADDR_A),
                            RemotingConnectException(ADDR_B)], async_retry=2)
    _client(p).sleep_ms = 60
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 100)
    _wait_done(cb)
    client = _client(p)
    assert client.attempts_count == 2          # 第二轮就把预算花光了
    assert client.timeouts[0] == 100
    assert 0 < client.timeouts[1] < client.timeouts[0]
    assert isinstance(cb.errors[0], RemotingConnectException)


def test_operation_fail_errors_are_wrapped_by_type():
    """Java operationFail 的三分支：按异常类型包装文案，TooMuchRequest 不重试。"""
    cases = [
        (RemotingSendRequestException(ADDR_A, "io fail"), "send request failed", True),
        (RemotingTimeoutException(ADDR_A, 3000), "wait response timeout", True),
        (RemotingConnectException(ADDR_A), "unknown reason", True),
        (RemotingTooMuchRequestException("too fast"), "unknown reason", False),
    ]
    for error, text, will_retry in cases:
        p = _producer(outcomes=[_Deliver(error=error)] * 3, async_retry=2)
        cb = _Callback()
        p.send_async(Message("TopicTest", b"x"), cb, 3000)
        _wait_done(cb)
        client = _client(p)
        assert text in str(cb.errors[0]), "%s -> %s" % (type(error).__name__, cb.errors[0])
        assert isinstance(cb.errors[0], MQClientException)
        assert cb.errors[0].cause is error
        expected = 3 if will_retry else 1
        assert client.attempts_count == expected, (type(error).__name__, client.attempts)


# ---------------------------------------------------------------- 定点发送
def test_fixed_mq_retry_stays_on_the_same_broker():
    """Java send(msg, mq, cb, timeout) 传下去的 topicPublishInfo 是 null → 原地重试。"""
    p = _producer(queues=[_mq("broker-a", 0)],
                  outcomes=[RemotingConnectException(ADDR_A),
                            RemotingConnectException(ADDR_A)], async_retry=2)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-a", 3))
    _wait_done(cb)
    client = _client(p)
    assert client.attempts_count == 3
    assert set(client.brokers) == {"broker-a"}
    assert [a for a, _, _, _ in client.attempts] == [ADDR_A] * 3
    assert len(set(client.opques)) == 3


def test_fixed_mq_runs_check_forbidden_hook_in_async_mode():
    from rocketmq.client.hook import CheckForbiddenContext

    seen = []

    class _Forbidden:
        def hook_name(self):
            return "forbidden-spy"

        def check_forbidden(self, context: CheckForbiddenContext) -> None:
            seen.append(context.communication_mode)

    p = _producer()
    p.register_check_forbidden_hook(_Forbidden())
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-a", 0))
    _wait_done(cb)
    assert cb.results and seen == [CommunicationMode.ASYNC]


def test_pinned_send_refreshes_the_route_to_resolve_the_address():
    """Java ``sendKernelImpl:919-924``：发布地址表查不到时按 topic 刷一次路由再解析。

    定点发送（调用方直接给 mq）不会在 sendDefaultImpl 里取发布信息，所以这一步是它
    唯一的路由来源 —— 少了这一步，第一次定点发送必然带着空地址去连。
    """
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-cold", {0: ADDR_COLD})]
    p = _producer(cold_brokers=["broker-cold"], route=route)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-cold", 0))
    _wait_done(cb)
    client = _client(p)
    assert [a for a, _, _, _ in client.attempts] == [ADDR_COLD]
    assert cb.results, "解析到地址后这一笔要发出去"


def test_unresolvable_broker_reports_not_exist_through_callback():
    """刷完路由还是没有这台 broker：Java ``sendKernelImpl:1100`` 的 "The broker[..] not exist"。"""
    p = _producer(cold_brokers=["broker-cold"], route=TopicRouteData())
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-cold", 0))
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], MQClientException)
    assert str(cb.errors[0]) == "The broker[broker-cold] not exist"


def test_async_kernel_timeout_gate_fails_before_the_request_goes_out():
    """Java ``sendKernelImpl:1043-1046``：ASYNC 分支自己的总闸。

    钩子/压缩/路由都是这条链的耗时，预算被它们吃光时不再发起请求，回调拿到
    ``RemotingTooMuchRequestException("sendKernelImpl call timeout")`` 且**不重试**。
    """
    hook = _RecordingHook()

    class _Slow:
        def hook_name(self):
            return "slow"

        def send_message_before(self, context: SendMessageContext) -> None:
            time.sleep(0.2)

        def send_message_after(self, context: SendMessageContext) -> None:
            hook.after.append(context)

    p = _producer()
    p.register_send_message_hook(_Slow())
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 50, _mq("broker-a", 0))
    _wait_done(cb)
    assert _client(p).attempts_count == 0, "预算用尽后不能再发请求"
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "sendKernelImpl call timeout" in str(cb.errors[0])
    # Java :1088 的 catch 先跑 hook.after 再抛，所以 after 一定拿到这个异常
    assert len(hook.after) == 1
    assert isinstance(hook.after[0].exception, RemotingTooMuchRequestException)


def test_check_forbidden_rejection_reaches_the_callback():
    from rocketmq.client.exception import MQClientException as ClientExc

    class _Block:
        def hook_name(self):
            return "forbidden-block"

        def check_forbidden(self, context) -> None:
            raise ClientExc("forbidden by hook")

    p = _producer()
    p.register_check_forbidden_hook(_Block())
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert "forbidden by hook" in str(cb.errors[0])


# ---------------------------------------------------------------- 路由与批量
def test_missing_route_reports_not_found_topic_through_callback():
    p = _producer(outcomes=[], no_route=True, queues=[])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], MQClientException)
    assert cb.errors[0].response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION


def test_batch_messages_fall_back_to_the_sync_batch_kernel():
    """有意偏离 Java：本实现的批量发送只有同步内核，但同样不阻塞调用方、回调照样触发。"""
    p = _producer()
    calls = []

    def _fake_batch(msgs, mq=None, timeout_millis=None):
        calls.append((len(msgs), timeout_millis))
        return SendResult(SendStatus.SEND_OK, msg_id="b" * 32, message_queue=_mq("broker-a", 0))

    p._send_batch = _fake_batch      # type: ignore[assignment]
    cb = _Callback()
    p.send_async([Message("TopicTest", b"1"), Message("TopicTest", b"2")], cb, 1500)
    _wait_done(cb)
    assert calls and calls[0][0] == 2
    assert cb.results[0].msg_id == "b" * 32


# ---------------------------------------------------------------- 线程名 / 生命周期
def test_executor_thread_names_follow_java(monkeypatch):
    """Java 的 ThreadFactoryImpl 名字形如 ``AsyncSenderExecutor_1``，序号从 **1** 开始。

    这里把 ``cpu_count`` 钉成 1：池是 core==max==核数（Java
    ``DefaultMQProducerImpl:134-140``），单核下两次投递才复用同一个 worker。
    多核机器上不会复用 —— 那是 Java ``ThreadPoolExecutor`` 的真实行为，见
    ``test_workers_grow_one_thread_per_submit_until_core``。
    """
    monkeypatch.setattr(producer_module.os, "cpu_count", lambda: 1)
    p = _producer()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert p._async_sender_executor.worker_count() == 1
    assert p._callback_executor.worker_count() == 1
    assert _client(p).build_threads == ["AsyncSenderExecutor_1"]
    assert cb.threads == ["NettyClientPublicExecutor_1"]
    cb2 = _Callback()
    p.send_async(Message("TopicTest", b"y"), cb2, 3000)
    _wait_done(cb2)
    # core == max == 1 且没排队：复用同一个 worker，不会像原先那样一次发送一个线程
    assert _client(p).build_threads == ["AsyncSenderExecutor_1", "AsyncSenderExecutor_1"]
    assert cb2.threads == ["NettyClientPublicExecutor_1"]


def test_workers_grow_one_thread_per_submit_until_core(monkeypatch):
    """core==max==4：Java ``ThreadPoolExecutor`` 在 ``poolSize < corePoolSize`` 时**每个任务
    新建一条线程**（哪怕别的 worker 正闲着），序号从 1 递增；到了 core 才不再建。

    断的是"建了几条、叫什么"，不是"任务落在哪条上"：``notify`` 叫醒的是先在等待的那个
    worker，所以任务归属本身是竞态的。
    """
    monkeypatch.setattr(producer_module.os, "cpu_count", lambda: 4)
    p = _producer()
    for i in range(4):
        cb = _Callback()
        p.send_async(Message("TopicTest", ("m%d" % i).encode()), cb, 3000)
        _wait_done(cb)
        assert cb.threads and cb.threads[0].startswith("NettyClientPublicExecutor_")
    assert _thread_names(p._async_sender_executor) == [
        "AsyncSenderExecutor_%d" % i for i in (1, 2, 3, 4)]
    assert _thread_names(p._callback_executor) == [
        "NettyClientPublicExecutor_%d" % i for i in (1, 2, 3, 4)]
    cb5 = _Callback()
    p.send_async(Message("TopicTest", b"m5"), cb5, 3000)
    _wait_done(cb5)
    # 到 core 之后不再新建线程，任务从队列里被已有 worker 取走
    assert p._async_sender_executor.worker_count() == 4
    assert p._callback_executor.worker_count() == 4
    assert _thread_names(p._async_sender_executor) == [
        "AsyncSenderExecutor_%d" % i for i in (1, 2, 3, 4)]


def test_shutdown_stops_accepting_new_work():
    p = _producer()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    p._started = False
    with pytest.raises(MQClientException):
        p.send_async(Message("TopicTest", b"y"), _Callback(), 3000)


def test_callback_survives_after_callback_pool_is_gone():
    """Java executeInvokeCallback：池不可用（关了/被拒）就 **runInThisThread**，回调不能丢。"""
    p = _producer()
    pool = p._callback_executor
    pool.shutdown(wait=True)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert cb.results
    assert cb.threads and not cb.threads[0].startswith("NettyClientPublicExecutor_")
