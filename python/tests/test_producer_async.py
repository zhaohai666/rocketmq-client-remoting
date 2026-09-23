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
from rocketmq.client.backpressure import MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE
from rocketmq.client.consume_executor import ConsumeExecutor
from rocketmq.client.exception import (ClientErrorCode, MQBrokerException,
                                       MQClientException, RequestTimeoutException)
from rocketmq.client.hook import CommunicationMode, SendMessageContext
from rocketmq.client.mq_client import MQClientInstance, TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.message_client_id_setter import get_uniq_id
from rocketmq.common.message_const import MessageConst
from rocketmq.common.message_decoder import decode_batch_messages
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
UNIQ = MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX


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


def test_send_batch_writes_client_ids_before_encoding_the_body():
    """对应 Java DefaultMQProducer.batch():1172-1184 的**顺序**：逐条 checkMessage →
    逐条 setUniqID → 逐条拼命名空间 → 给批量本身 setUniqID → 才 encode()。

    顺序错或干脆不写（本端口原先就是这样）会让批量 body 里的每条子消息都没有 UNIQ_KEY：
    消费端拿到没有客户端 ID 的消息、轨迹控制台串不起发送侧与消费侧，而 SendResult.msgId
    只能退化成 broker 的 offsetMsgId —— 单看「发成功了没」完全看不出来。
    """
    p = _producer()
    p.namespace = "BatchNs"
    sent = []

    def _fake_send(group, msg, mq, timeout, sys_flag, unit_mode=False):
        sent.append(msg)
        return SendResult(SendStatus.SEND_OK, msg_id="b" * 32,
                          message_queue=_mq("broker-a", 0))

    p._mq_client.send_message = _fake_send          # type: ignore[assignment]
    msgs = [Message("TopicTest", b"one"), Message("TopicTest", b"two")]
    result = p._send_batch(msgs)
    assert result.send_status == SendStatus.SEND_OK
    assert len(sent) == 1, "一批一个请求"
    batch = sent[0]
    # ① 每条子消息各有自己的 UNIQ_KEY，批量本身也有（broker 判 inner-batch 用得到）
    sub_ids = [m.get_property(UNIQ) for m in batch.messages]
    assert all(sub_ids) and len(set(sub_ids)) == len(sub_ids), sub_ids
    assert get_uniq_id(batch) and get_uniq_id(batch) not in sub_ids
    # ② 编码在写 ID **之后**：解出来的每条子消息都带着自己的 UNIQ_KEY。
    #    （批量 body 是 6 段轻量格式，不带 topic，所以 ID 是唯一能证明顺序的证据。）
    inner = decode_batch_messages(batch.get_body())
    assert [m.get_property(UNIQ) for m in inner] == sub_ids
    assert [m.topic for m in batch.messages] == [p._with_namespace("TopicTest")] * 2
    # ③ 回调里的 msgId 是**批量自身**的 UNIQ_KEY（四语言统一口径；Java 在非 inner-batch
    #    时拼逐条 ID，本端口解析层拿不到子消息列表，理由见 _parse_send_response 的注释）
    response = RemotingCommand.create_response_command_with_header(ResponseCode.SUCCESS)
    response.ext_fields = {"msgId": "0" * 32, "queueId": "0", "queueOffset": "0"}
    parsed = MQClientInstance._parse_send_response(None, response, batch, _mq("broker-a", 0))
    assert parsed.msg_id == get_uniq_id(batch)
    assert parsed.msg_id != "0" * 32, "不能退化成 broker 的 offsetMsgId"
    assert "," not in parsed.msg_id
    # broker 回了 batchUniqId（inner-batch）时同样用它
    response.ext_fields = {"msgId": "0" * 32, "queueId": "0", "queueOffset": "0",
                           "batchUniqId": get_uniq_id(batch)}
    parsed = MQClientInstance._parse_send_response(None, response, batch, _mq("broker-a", 0))
    assert parsed.msg_id == get_uniq_id(batch)


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


# ---------------------------------------------------------------- 异步发送背压
# 对端是 Java DefaultMQProducerImpl:635-682（executeAsyncMessageSend 的两道闸）与
# :577-633（BackpressureSendCallBack 的归还）。默认关闭，所以上面那批用例一行都不用改。
# 字节闸的地板值是 1M（Java :148-153），所以这里只能拿"1M 少掉多少"来断言，
# 不能把上限配成几百字节 —— 那会被夹回 1M。
def _backpressure_producer(num: int = 10, size: int = MIN_ASYNC_SEND_SIZE,
                           **kwargs) -> DefaultMQProducer:
    """开背压的生产者。字节闸夹到地板值 1M：够用，又能把「扣了多少字节」算清楚。"""
    p = _producer(**kwargs)
    p.set_enable_backpressure_for_async_mode(True)
    p.set_back_pressure_for_async_send_num(num)
    p.set_back_pressure_for_async_send_size(size)
    return p


def test_backpressure_defaults_match_java_and_stay_out_of_the_way():
    """Java DefaultMQProducer:169/175/181 —— 默认关，1024 条 / 100M 字节。"""
    p = DefaultMQProducer("GID_async_test")
    assert p.is_enable_backpressure_for_async_mode() is False
    assert p.get_back_pressure_for_async_send_num() == 1024
    assert p.get_back_pressure_for_async_send_size() == 100 * 1024 * 1024
    assert p.get_semaphore_async_send_num_available_permits() == 1024
    assert p.get_semaphore_async_send_size_available_permits() == 100 * 1024 * 1024
    # 关着的时候一笔发送既不扣也不还许可
    p._create_async_executors()
    p._mq_client = _FakeAsyncClient([_mq("broker-a", 0)], None, False, None, None, ())
    p._started = True
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 5000), cb, 3000)
    _wait_done(cb)
    assert p.get_semaphore_async_send_num_available_permits() == 1024
    assert p.get_semaphore_async_send_size_available_permits() == 100 * 1024 * 1024


def test_backpressure_num_floor_is_ten_and_size_floor_is_one_meg():
    """Java DefaultMQProducerImpl:141-153 与 :1385/1402 —— 两个地板值都夹得住。"""
    p = DefaultMQProducer("GID_async_test")
    p.set_back_pressure_for_async_send_num(1)
    p.set_back_pressure_for_async_send_size(1024)
    assert p.get_back_pressure_for_async_send_num() == MIN_ASYNC_SEND_NUM
    assert p.get_back_pressure_for_async_send_size() == MIN_ASYNC_SEND_SIZE
    assert p.get_semaphore_async_send_num_available_permits() == MIN_ASYNC_SEND_NUM
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_num_gate_fails_with_java_message_and_sends_nothing():
    """Java :654-658 —— 条数拿不到就回调，一次请求都不发（预算也被闸自己花光）。"""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    assert p._semaphore_async_send_num.try_acquire(10, 0) is True   # 占满在途
    began = time.monotonic()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 300)
    _wait_done(cb)
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert str(cb.errors[0]).endswith("send message tryAcquire semaphoreAsyncNum timeout")
    assert _client(p).attempts_count == 0
    assert int((time.monotonic() - began) * 1000) >= 250, "没等到超时就把失败交出去了"
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE
    p._semaphore_async_send_num.release(10)
    gate.set()


def test_size_gate_fails_with_its_own_message_and_gives_the_num_permit_back():
    """Java :667-671 —— 字节闸没过时，**已经拿到**的条数许可必须归还。"""
    p = _backpressure_producer(num=10)
    assert p._semaphore_async_send_size.try_acquire(MIN_ASYNC_SEND_SIZE, 0) is True
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 10), cb, 200)
    _wait_done(cb)
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "send message tryAcquire semaphoreAsyncSize timeout" in str(cb.errors[0])
    assert p.get_semaphore_async_send_num_available_permits() == 10, "条数许可漏还了"
    assert _client(p).attempts_count == 0
    p._semaphore_async_send_size.release(MIN_ASYNC_SEND_SIZE)


def test_permits_are_borrowed_per_in_flight_send_and_given_back():
    """一条在途发送 = 1 个条数许可 + body.length 个字节许可；回调后两者都回来。"""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 400), cb, 3000)
    _wait_attempts(_client(p), 1)
    assert p.get_semaphore_async_send_num_available_permits() == 9
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE - 400
    gate.set()
    _wait_done(cb)
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_null_or_empty_body_still_costs_one_size_permit():
    """Java :642 —— ``getBody() == null ? 1 : getBody().length``。

    两种「没有 body」在链路上都被 ``Validators.checkMessage`` 先拒了（body 为 null /
    长度为 0，四端口一致），所以这里断的是计价本身，而不是走一遍发送。
    """
    assert producer_module._back_pressure_msg_len(Message("T", b"")) == 1
    no_body = Message("T", b"x")
    no_body.body = None
    assert producer_module._back_pressure_msg_len(no_body) == 1
    # 批量按每条累加，其中空 body 那条同样算 1
    assert producer_module._back_pressure_msg_len(
        [Message("T", b"a" * 3), Message("T", b"")]) == 4


def test_failure_also_gives_the_permits_back():
    """归还挂在 on_exception 上（Java semaphoreProcessor 在两个回调里都跑），失败不能漏。"""
    p = _backpressure_producer(
        num=10, outcomes=[_Deliver(error=MQBrokerException(
            ResponseCode.SYSTEM_ERROR, "store error"))])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 300), cb, 3000)
    _wait_done(cb)
    assert cb.errors
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_retry_chain_releases_once_not_once_per_attempt():
    """重试链上只有一份许可（一次发送一笔），终点归还一次；还多次会把容量虚增。"""
    p = _backpressure_producer(
        num=10, outcomes=[_Deliver(error=RemotingTimeoutException(ADDR_A, 1)),
                          _Deliver(error=RemotingTimeoutException(ADDR_B, 1)),
                          _Deliver(error=RemotingTimeoutException(ADDR_A, 1))])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 300), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 3, "这条链本应重试两轮后终止"
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_gate_waits_for_a_permit_instead_of_failing_early():
    """信号量是**等**到超时为止，不是看一眼不够就报错 —— 这才是背压限流的意义。"""
    gate = threading.Event()
    # 条数闸的地板值是 10（Java :141-146），所以"占满"要占 10 笔而不是任意小数
    p = _backpressure_producer(num=MIN_ASYNC_SEND_NUM, block=gate)
    held = []
    for _ in range(MIN_ASYNC_SEND_NUM):
        cb = _Callback()
        held.append(cb)
        p.send_async(Message("TopicTest", b"x"), cb, 8000)
    _wait_attempts(_client(p), 1)
    assert p.get_semaphore_async_send_num_available_permits() == 0
    third = _Callback()
    began = time.monotonic()

    def _send():
        p.send_async(Message("TopicTest", b"y"), third, 8000)

    t = threading.Thread(target=_send)
    t.start()
    time.sleep(0.2)
    assert t.is_alive(), "调用方不该在许可还回来之前就返回"
    gate.set()
    t.join(6.0)
    assert not t.is_alive()
    assert int((time.monotonic() - began) * 1000) >= 150
    gate.set()
    for cb in held:
        _wait_done(cb)
    _wait_done(third)
    assert third.results, "等到许可后这一笔应当发出去"
    assert p.get_semaphore_async_send_num_available_permits() == MIN_ASYNC_SEND_NUM


def test_runtime_resize_keeps_the_in_flight_share():
    """Java DefaultMQProducerTest:593-595 的那条断言：空闲许可 + 在途份数 == 新配置。"""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    callbacks = []
    for _ in range(5):
        cb = _Callback()
        callbacks.append(cb)
        p.send_async(Message("TopicTest", b"x"), cb, 8000)
    assert p.get_semaphore_async_send_num_available_permits() == 5
    p.set_back_pressure_for_async_send_num(15)
    assert p.get_semaphore_async_send_num_available_permits() + 5 == 15
    gate.set()
    for cb in callbacks:
        _wait_done(cb)
    assert p.get_semaphore_async_send_num_available_permits() == 15


def test_growing_capacity_wakes_a_blocked_sender():
    """本实现改容量不丢等待者（Java 换对象会把等待者留在旧信号量上等自己的超时）——
    调大容量之后，正卡在闸上的调用方应当被叫醒并把这笔发出去。"""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    for _ in range(10):
        p.send_async(Message("TopicTest", b"x"), _Callback(), 8000)
    assert p.get_semaphore_async_send_num_available_permits() == 0
    blocked = _Callback()
    t = threading.Thread(target=lambda: p.send_async(
        Message("TopicTest", b"y"), blocked, 8000))
    t.start()
    time.sleep(0.2)
    assert t.is_alive()
    p.set_back_pressure_for_async_send_num(11)     # 凭空多出 1 条容量
    t.join(6.0)
    assert not t.is_alive(), "扩容后调用方还卡在闸上"
    gate.set()
    _wait_done(blocked)
    assert blocked.results


def test_queue_full_runs_inline_when_backpressure_is_on():
    """Java :675-681：队满时**没有**背压才抛 "executor rejected"；有背压就就地跑完这一笔
    （许可已经扣掉了，抛异常会让它悬着）。"""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    p._async_sender_executor = ConsumeExecutor(1, 1, thread_name_prefix="AsyncSenderExecutor",
                                              max_queue_size=1, thread_name_sep="_",
                                              thread_index_from=1)
    running = _Callback()
    p.send_async(Message("TopicTest", b"1"), running, 8000)
    _wait_attempts(_client(p), 1)
    p.send_async(Message("TopicTest", b"2"), _Callback(), 8000)     # 进队列
    for _ in range(200):
        if p._async_sender_executor.queued_count() == 1:
            break
        time.sleep(0.005)
    assert p._async_sender_executor.queued_count() == 1
    # 第三笔：池子拒收 → 就地跑，会占住调用方直到 gate 打开
    inline = _Callback()
    returned = threading.Event()

    def _third():
        p.send_async(Message("TopicTest", b"3"), inline, 8000)
        returned.set()

    t = threading.Thread(target=_third)
    t.start()
    time.sleep(0.2)
    assert not returned.is_set(), "就地跑应当占住调用方"
    gate.set()
    t.join(6.0)
    assert returned.is_set(), "队满不该把异常甩给调用方"
    _wait_done(running)
    _wait_done(inline)
    assert _client(p).attempts_count >= 3
    assert p.get_semaphore_async_send_num_available_permits() == 10


def test_batch_async_charges_the_sum_of_body_lengths():
    """批量是 Java 没有的异步入口，这里按每条累加，别让一整批只算一笔的钱。"""
    p = _backpressure_producer(num=10)
    seen = {}
    real_inner = p._send_async_inner

    def _spy(msgs, mq, callback, timeout):
        seen["size"] = p.get_semaphore_async_send_size_available_permits()
        seen["num"] = p.get_semaphore_async_send_num_available_permits()
        return real_inner(msgs, mq, callback, timeout)

    p._send_async_inner = _spy      # type: ignore[assignment]
    calls = []

    def _fake_batch(msgs, mq=None, timeout_millis=None):
        calls.append(len(msgs))
        return SendResult(SendStatus.SEND_OK, msg_id="b" * 32,
                          message_queue=_mq("broker-a", 0))

    p._send_batch = _fake_batch      # type: ignore[assignment]
    cb = _Callback()
    p.send_async([Message("TopicTest", b"a" * 100), Message("TopicTest", b"b" * 250)],
                 cb, 5000)
    _wait_done(cb)
    assert calls == [2]
    assert seen["size"] == MIN_ASYNC_SEND_SIZE - 350
    assert seen["num"] == 9
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE
    assert p.get_semaphore_async_send_num_available_permits() == 10


def test_request_goes_through_the_same_gate_and_leaks_nothing():
    """request() 内部就是这条 ASYNC 链（Java 亦然），所以它同样受背压约束、同样要归还。"""
    p = _backpressure_producer(num=10)
    client = _client(p)
    client.client_id = "fake-client-id"
    delivered = {}

    def _capture(addr, request, msg, mq, timeout, on_complete):
        delivered["opaque"] = request.opaque
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    client.send_message_async = _capture      # type: ignore[assignment]
    # 应答通道本来就不存在（fake 客户端没有 326 推送），所以按 Java 语义抛等应答超时
    with pytest.raises(RequestTimeoutException):
        p.request(Message("TopicTest", b"q" * 30), 300)
    assert delivered, "request() 应当走异步链"
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE
