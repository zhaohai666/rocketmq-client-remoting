# -*- coding: utf-8 -*-
"""Unit tests of the asynchronous send chain (send_async(msg, callback, timeout) and the remoting
asynchronous request / failure retry underneath it).

What they watch are these real behaviours (not "asynchronous = spawn a thread"):

  * ``send_async`` **does not block the caller**: all the preparation happens in
    ``AsyncSenderExecutor_N`` (pool size = number of CPU cores, bounded queue of 50000), and only
    when the queue is full is ``executor rejected`` thrown to the caller;
  * The elapsed time only starts counting after the task leaves the queue: if the budget was eaten by
    the queueing it directly calls back with
    ``RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")`` and sends not a single
    request;
  * In the default send loop ASYNC fixes ``timesTotal`` at 1 -- the outer loop does not switch broker;
    switching broker only happens on the failure retry chain, whose upper bound is
    ``retryTimesWhenSendAsyncFailed``;
  * The retry **reuses the same RemotingCommand** (the request is built only once) and takes a new
    opaque -- the old opaque is still sitting in the responseTable waiting for its timeout, and
    reusing it would let the replies of the two attempts cross over;
  * The timeout budget is the **shared** remaining time (``timeoutMillis - cost``), not a fresh budget
    for each attempt;
  * Only the remoting-layer exceptions are retried: ``RemotingTimeoutException`` /
    ``RemotingSendRequestException`` are wrapped into ``MQClientException("wait response timeout,
    cost=…")`` / ``("send request failed")``, the other ``RemotingException`` into ``("unknown
    reason")``, and ``RemotingTooMuchRequestException`` is not retried at all;
  * CAUTION: **a response has already been received** but the broker replied with an error code
    (``MQBrokerException``) means **no retry and no wrapping either** -- asynchronous sending does not
    look at ``retryResponseCodes``, which is completely different from synchronous sending;
  * With a pinned send (carrying an mq) the topic publish info passed down is null, so the retry
    **goes back to the very same broker**;
  * The user callback and ``SendMessageHook.after`` run on ``NettyClientPublicExecutor_N`` (callbacks
    are uniformly submitted to the common executor and only run in place when the pool is
    unavailable), and an exception thrown by the user callback is swallowed so that it cannot take a
    worker down with it.

The live part (the message really lands on the broker and the callback gets SEND_OK) is section 1
of ``verify_message_types.py``.
"""
from __future__ import annotations

import threading
import time
from typing import List, Optional

import pytest

from client import producer as producer_module
from client.backpressure import MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE
from client.consume_executor import ConsumeExecutor
from client.exception import (ClientErrorCode, MQBrokerException,
                                       MQClientException, RequestTimeoutException)
from client.hook import CommunicationMode, SendMessageContext
from client.mq_client import MQClientInstance, TopicPublishInfo
from client.producer import DefaultMQProducer
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageQueue
from common.message_client_id_setter import get_uniq_id
from common.message_const import MessageConst
from common.message_decoder import decode_batch_messages
from remoting.exception import (RemotingConnectException,
                                         RemotingSendRequestException,
                                         RemotingTimeoutException,
                                         RemotingTooMuchRequestException)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
ADDR_COLD = "127.0.0.1:10913"
UNIQ = MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX


def _mq(broker: str, queue_id: int = 0) -> MessageQueue:
    return MessageQueue("TopicTest", broker, queue_id)


class _Deliver:
    """A result delivered by the fake through the **callback** (as opposed to an exception raised in place)."""

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error


class _Callback:
    """A test double that records the callbacks."""

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
    """An MQClientInstance double, stitched onto the three interfaces the asynchronous send uses.

    Each entry of ``outcomes``: BaseException -> **raised in place** from ``send_message_async``
    (a synchronous failure); ``_Deliver`` -> goes through the ``on_complete`` callback.
    Once the list is used up everything replies SEND_OK.
    """

    def __init__(self, queues: List[MessageQueue], outcomes: Optional[list] = None,
                 no_route: bool = False, block: Optional[threading.Event] = None,
                 route=None, cold_brokers=(), namesrv_addrs: Optional[List[str]] = None):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = list(queues)
        self.outcomes = list(outcomes or [])
        self.no_route = no_route
        # MQClientInstance really has this field, and it is what validateNameServerSetting
        # reads (to tell "no name server configured at all" from "this topic has no route" when the
        # route cannot be obtained).
        self.name_server_addrs = (["127.0.0.1:9876"] if namesrv_addrs is None
                                  else list(namesrv_addrs))
        self.block = block
        # The publish address **flat table**: brokerName -> master address.
        # A broker on the cold list has "no master" and never enters the table at all -- so the
        # publish address table cannot find it.
        self.publish_addr_table = {"broker-a": ADDR_A, "broker-b": ADDR_B}
        for name in cold_brokers:
            self.publish_addr_table.pop(name, None)
        # the return value of get_topic_route_data (used by the topic-based route refresh of the pinned send chain)
        self.route = route
        self.attempts: List[tuple] = []   # (addr, broker_name, opaque, timeout)
        self.built: List[int] = []        # the number of mqs build_send_request received (used for counting)
        self.build_threads: List[str] = []   # the name of the thread that built the request
        self.sleep_ms = 0                 # the cost of each attempt, used to verify the "shared timeout budget"
        self.lock = threading.Lock()

    # --- the MQClientInstance interfaces the producer asynchronous chain touches ---
    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        if self.no_route:
            raise MQClientException("No route info of this topic: %s" % topic)
        if not self.publish.ok():
            raise MQClientException("Can not find Message Queue for topic: %s" % topic)
        return self.publish

    def find_broker_address_in_publish(self, broker_name):
        return self.publish_addr_table.get(broker_name)

    def publish_addr_for(self, broker_name, topic):
        """The same shape as the real thing: look it up (master only) -> refresh the route
        once by topic -> look it up again -> report not exist.

        The double must **copy verbatim** this step instead of simplifying it:
        ``test_pinned_send_refreshes_the_route_...`` tests exactly "the address only shows up
        after the route was refreshed", so collapsing the two steps into one would make that test
        case meaningless.
        """
        addr = self.find_broker_address_in_publish(broker_name)
        if not addr:
            self.update_topic_route_info_from_name_server(topic)
            addr = self.find_broker_address_in_publish(broker_name)
        if not addr:
            raise MQClientException("The broker[%s] not exist" % broker_name)
        return addr

    def update_topic_route_info_from_name_server(self, topic, is_default=False):
        """Refresh the route once by topic: overwrite the brokers of the route that **have a
        master** into the publish address flat table.

        This step of the real thing writes exactly the flat table ``broker_addr_table``; a broker
        without a master is not written, which means it is still not found after the refresh.
        """
        for broker_data in (getattr(self.route, "broker_datas", None) or []):
            if broker_data.broker_addrs.get(0):
                self.publish_addr_table[broker_data.broker_name] = broker_data.broker_addrs[0]

    def get_topic_route_data(self, topic):
        return self.route

    def build_send_request(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                           default_topic=None, default_topic_queue_nums=None):
        with self.lock:
            self.built.append(mq.broker_name)
            # Building the request happens on the AsyncSenderExecutor thread -- record the thread name along the
            # way for the assertions
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

    # --- for the assertions ---
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
    """A SendMessageHook double: it records how many times before/after ran and what after carried."""

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
              block=None, queue_capacity=50000, route=None, cold_brokers=(),
              namesrv_addrs=None) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_async_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(2)
    p.retry_times_when_send_async_failed = async_retry
    p.async_sender_queue_capacity = queue_capacity
    # start() creates a real client; here only the three things the asynchronous chain needs are added:
    # the started flag plus the two pools
    p._create_async_executors()
    p._mq_client = _FakeAsyncClient(
        [_mq("broker-a", 0), _mq("broker-b", 1)] if queues is None else list(queues),
        outcomes, no_route, block, route, cold_brokers, namesrv_addrs=namesrv_addrs)
    p._started = True
    return p


def _client(p: DefaultMQProducer) -> _FakeAsyncClient:
    return p._mq_client


def _thread_names(pool: ConsumeExecutor) -> List[str]:
    """The thread name of the current worker in the pool (a product of ``ThreadFactoryImpl``)."""
    return [t.name for t in pool._threads]


def _wait_done(cb: _Callback, timeout: float = 5.0) -> None:
    assert cb.done.wait(timeout), "the asynchronous callback never fired"


def _wait_attempts(client: _FakeAsyncClient, n: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while client.attempts_count < n and time.monotonic() < deadline:
        time.sleep(0.005)


def _wait_num_permits(p: DefaultMQProducer, want: int, timeout: float = 5.0) -> None:
    """The count permits are returned inside the callback thread: for the queued entries whose callbacks nobody
    waits for, it has to poll up to a bounded timeout."""
    deadline = time.monotonic() + timeout
    while p.get_semaphore_async_send_num_available_permits() != want and time.monotonic() < deadline:
        time.sleep(0.005)
    assert p.get_semaphore_async_send_num_available_permits() == want, "the count permits were not all returned"


# ---------------------------------------------------------------- the caller is not blocked
def test_send_async_returns_before_the_request_is_issued():
    """The task is merely submitted to the asyncSenderExecutor and the caller returns immediately."""
    gate = threading.Event()
    p = _producer(block=gate)
    cb = _Callback()
    t0 = time.monotonic()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    assert elapsed_ms < 500, "send_async blocked the caller for %dms" % elapsed_ms
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
    # the worker thread names look like AsyncSenderExecutor_1 / NettyClientPublicExecutor_1
    assert seen["callback_thread"].startswith("NettyClientPublicExecutor_")
    assert seen["callback_thread"] != caller
    assert _client(p).attempts_count == 1


# ---------------------------------------------------------------- the success path
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
    # the hook context of the ASYNC branch: communicationMode has to be marked ASYNC
    assert hook.before[0].communication_mode == CommunicationMode.ASYNC
    assert hook.after[0].send_result is cb.results[0]
    assert hook.after[0].exception is None


def test_user_callback_exception_does_not_kill_the_pool():
    """A catch-all wraps onSuccess/onException: an exception in the callback must not take the worker down."""
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


# ---------------------------------------------------------------- queueing and elapsed time
def test_zero_timeout_fails_without_sending_anything():
    """The runnable that an asynchronous task executes once it leaves the queue: timeout <= costTime goes straight to onException."""
    p = _producer()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 0)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "DEFAULT ASYNC send call timeout" in str(cb.errors[0])


def test_bounded_queue_rejects_the_caller_not_the_callback():
    """When the executor rejects the task it throws ``MQClientException("executor rejected")``
    straight to the caller.

    Here the pool is squeezed to 1 worker + 1 queue slot (the real pool is the number of CPU cores +
    50000), so that the chain "occupy the worker -> fill the queue -> the third one is rejected"
    runs fast and deterministically.
    """
    gate = threading.Event()
    p = _producer(block=gate)
    p._async_sender_executor = ConsumeExecutor(1, 1, thread_name_prefix="AsyncSenderExecutor",
                                              max_queue_size=1, thread_name_sep="_",
                                              thread_index_from=1)
    running = _Callback()
    p.send_async(Message("TopicTest", b"1"), running, 3000)
    _wait_attempts(_client(p), 1)          # the first task has occupied the only worker
    queued = _Callback()
    p.send_async(Message("TopicTest", b"2"), queued, 3000)   # goes into the queue
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


# ---------------------------------------------------------------- switching broker on retry
def test_connect_failure_retries_with_new_opaque_and_shrinking_budget():
    """The failure retry chain: a retry happens only while times<=retryTimesWhenSendAsyncFailed,
    every round switches broker and opaque and reuses the same request, and the timeout uses the
    shared remaining budget."""
    outcomes = [RemotingConnectException(ADDR_A), RemotingConnectException(ADDR_B),
                RemotingConnectException(ADDR_A)]
    p = _producer(outcomes=outcomes, async_retry=2)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    client = _client(p)
    assert client.attempts_count == 3          # 1 original + 2 retries
    assert len(client.built) == 1              # the request is built only once and reused across retries
    assert len(set(client.opques)) == 3        # every attempt takes a new opaque
    assert client.brokers[0] != client.brokers[1]   # it avoids the machine that just failed
    assert client.timeouts[1] <= client.timeouts[0]
    assert len(cb.results) == 0 and len(cb.errors) == 1
    # The path where the remoting call **raises the exception in place**: the exception goes to the
    # callback unchanged (not wrapped in an MQClientException) and needRetry=true. What gets wrapped
    # into "send request failed"/"unknown reason" is the path where the callback delivery fails (see
    # the two test cases below).
    assert isinstance(cb.errors[0], RemotingConnectException)


def test_retry_cap_zero_disables_failover():
    p = _producer(outcomes=[_Deliver(error=RemotingTimeoutException(ADDR_A, 3000))],
                  async_retry=0)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    # An upper bound of 0 retries: it sends once and then calls back
    assert _client(p).attempts_count == 1
    assert "wait response timeout" in str(cb.errors[0])


def test_broker_error_code_is_returned_as_is_without_retry():
    """CAUTION, different from synchronous sending: once the response is received
    needRetry=false, and the exception is **not wrapped** either.

    On the normal response path the ``MQBrokerException`` thrown by ``processSendResponse`` falls
    into ``catch (Exception e)`` -> with ``needRetry=false`` it is handed to the failure callback.
    """
    broker_error = MQBrokerException(ResponseCode.SYSTEM_BUSY, "broker busy")
    p = _producer(outcomes=[_Deliver(error=broker_error)], async_retry=2)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 1
    assert cb.errors[0] is broker_error


def test_retry_cap_is_read_from_config():
    """retry_times_when_send_async_failed used to be a field that was stored but never read."""
    p = _producer(outcomes=[RemotingConnectException(ADDR_A)] * 5, async_retry=4)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 5      # 1 + 4


def test_retry_budget_is_shared_not_per_attempt():
    """What the retry gets is ``timeoutMillis - cost``: the budget is **shared**, so the second
    attempt receives only the little that is left rather than yet another full timeout."""
    p = _producer(outcomes=[RemotingConnectException(ADDR_A),
                            RemotingConnectException(ADDR_B)], async_retry=2)
    _client(p).sleep_ms = 60
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 100)
    _wait_done(cb)
    client = _client(p)
    assert client.attempts_count == 2          # the second round already used up the budget
    assert client.timeouts[0] == 100
    assert 0 < client.timeouts[1] < client.timeouts[0]
    assert isinstance(cb.errors[0], RemotingConnectException)


def test_operation_fail_errors_are_wrapped_by_type():
    """The three branches of the failure wrapping: the wording follows the exception type, and TooMuchRequest is not retried."""
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


# ---------------------------------------------------------------- pinned send
def test_fixed_mq_retry_stays_on_the_same_broker():
    """The topicPublishInfo passed down by a pinned send is null -> it retries in place."""
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
    from client.hook import CheckForbiddenContext

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
    """When the publish address table does not find it, refresh the route once by topic and resolve
    again.

    A pinned send (the caller supplies the mq directly) does not fetch the publish info in the main
    send path, so this step is its only source of the route -- without it the very first pinned send
    would inevitably connect with an empty address.
    """
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-cold", {0: ADDR_COLD})]
    p = _producer(cold_brokers=["broker-cold"], route=route)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-cold", 0))
    _wait_done(cb)
    client = _client(p)
    assert [a for a, _, _, _ in client.attempts] == [ADDR_COLD]
    assert cb.results, "once the address is resolved this entry must go out"


def test_unresolvable_broker_reports_not_exist_through_callback():
    """Still no such broker after the route refresh: the callback has to receive "The broker[..] not exist"."""
    p = _producer(cold_brokers=["broker-cold"], route=TopicRouteData())
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000, _mq("broker-cold", 0))
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], MQClientException)
    assert str(cb.errors[0]) == "The broker[broker-cold] not exist"


def test_async_kernel_timeout_gate_fails_before_the_request_goes_out():
    """The master gate of the send kernel of the ASYNC branch.

    Hooks / compression / routing are all part of the cost of this chain; when the budget is eaten up
    by them no request is started, the callback receives
    ``RemotingTooMuchRequestException("sendKernelImpl call timeout")`` and it is **not retried**.
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
    assert _client(p).attempts_count == 0, "no request may be sent once the budget is exhausted"
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "sendKernelImpl call timeout" in str(cb.errors[0])
    # the catch of this chain runs hook.after before rethrowing, so after definitely gets this exception
    assert len(hook.after) == 1
    assert isinstance(hook.after[0].exception, RemotingTooMuchRequestException)


def test_check_forbidden_rejection_reaches_the_callback():
    from client.exception import MQClientException as ClientExc

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


# ---------------------------------------------------------------- routing and batching
def test_missing_route_reports_not_found_topic_through_callback():
    p = _producer(outcomes=[], no_route=True, queues=[])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], MQClientException)
    assert cb.errors[0].response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION


def test_no_name_server_address_reaches_the_callback_as_10004():
    """The behaviour of the asynchronous chain must not diverge from the synchronous one: with no name
    server address the callback has to get 10004 too.

    The asynchronous and synchronous chains take the same no-route branch (validateNameServerSetting
    first, then throw no-route), so both chains share the same decision; the asynchronous side used to
    wrap the exception into a new MQClientException and lose the code in the process, rewriting 10004
    into 10005.
    """
    p = _producer(outcomes=[], no_route=True, queues=[], namesrv_addrs=[])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 0
    assert isinstance(cb.errors[0], MQClientException)
    assert cb.errors[0].response_code == ClientErrorCode.NO_NAME_SERVER_EXCEPTION
    assert str(cb.errors[0]) == "No name server address, please set it."


def test_batch_messages_fall_back_to_the_sync_batch_kernel():
    """A batch send only has a synchronous kernel, which is a deliberate design: it does not block the caller and the callback still fires."""
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
    """The **order** a batch send must follow: checkMessage per entry -> setUniqID per entry ->
    build the namespace per entry -> setUniqID for the batch itself -> only then encode().

    Getting the order wrong, or not writing them at all, leaves every sub-message in the batch body
    without a UNIQ_KEY: the consumer gets messages without a client id, the trace console cannot
    connect the producer side with the consumer side, and SendResult.msgId can only degrade to the
    broker's offsetMsgId -- looking only at "did the send succeed" reveals nothing of this.
    """
    p = _producer()
    p.namespace = "BatchNs"
    sent = []

    def _fake_send(group, msg, mq, timeout, sys_flag, unit_mode=False,
                   default_topic=None, default_topic_queue_nums=None):
        sent.append(msg)
        return SendResult(SendStatus.SEND_OK, msg_id="b" * 32,
                          message_queue=_mq("broker-a", 0))

    p._mq_client.send_message = _fake_send          # type: ignore[assignment]
    msgs = [Message("TopicTest", b"one"), Message("TopicTest", b"two")]
    result = p._send_batch(msgs)
    assert result.send_status == SendStatus.SEND_OK
    assert len(sent) == 1, "one batch is one request"
    batch = sent[0]
    # (1) every sub-message has its own UNIQ_KEY, and so does the batch itself (the broker needs it to decide
    # inner-batch)
    sub_ids = [m.get_property(UNIQ) for m in batch.messages]
    assert all(sub_ids) and len(set(sub_ids)) == len(sub_ids), sub_ids
    assert get_uniq_id(batch) and get_uniq_id(batch) not in sub_ids
    # (2) the encoding happens **after** the IDs are written: every sub-message that is decoded carries its own UNIQ_KEY.
    #    (The batch body is the 6-segment lightweight format and carries no topic, so the ID is the only evidence of the order.)
    inner = decode_batch_messages(batch.get_body())
    assert [m.get_property(UNIQ) for m in inner] == sub_ids
    assert [m.topic for m in batch.messages] == [p._with_namespace("TopicTest")] * 2
    # (3) the msgId in the callback is the UNIQ_KEY of the **batch itself** (for a non inner-batch the
    #    parsing layer of this port cannot get the list of sub-messages and therefore cannot assemble
    #    the IDs one by one; see the comment in _parse_send_response for the reason)
    response = RemotingCommand.create_response_command_with_header(ResponseCode.SUCCESS)
    response.ext_fields = {"msgId": "0" * 32, "queueId": "0", "queueOffset": "0"}
    parsed = MQClientInstance._parse_send_response(None, response, batch, _mq("broker-a", 0))
    assert parsed.msg_id == get_uniq_id(batch)
    assert parsed.msg_id != "0" * 32, "it must not degrade into the broker's offsetMsgId"
    assert "," not in parsed.msg_id
    # the same one is used when the broker replied with a batchUniqId (inner-batch)
    response.ext_fields = {"msgId": "0" * 32, "queueId": "0", "queueOffset": "0",
                           "batchUniqId": get_uniq_id(batch)}
    parsed = MQClientInstance._parse_send_response(None, response, batch, _mq("broker-a", 0))
    assert parsed.msg_id == get_uniq_id(batch)


# ---------------------------------------------------------------- thread names / lifecycle
def test_executor_thread_names_use_the_numbered_prefix(monkeypatch):
    """The worker thread name looks like ``AsyncSenderExecutor_1``, and the number starts at **1**.

    Here ``cpu_count`` is pinned to 1: the pool is core==max==number of cores, and only on a single
    core do two submissions reuse the same worker.
    On a multi-core machine they are not reused -- that is the real behaviour of the pool creating
    threads step by step up to core, see
    ``test_workers_grow_one_thread_per_submit_until_core``.
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
    # core == max == 1 with nothing queued: the same worker is reused, instead of one thread per send as before
    assert _client(p).build_threads == ["AsyncSenderExecutor_1", "AsyncSenderExecutor_1"]
    assert cb2.threads == ["NettyClientPublicExecutor_1"]


def test_workers_grow_one_thread_per_submit_until_core(monkeypatch):
    """core==max==4: while the pool has not reached core (``poolSize < corePoolSize``) **every
    task creates a new thread** (even when another worker is idle), with the number incrementing
    from 1; once core is reached it stops creating.

    What it pins down is "how many threads were created and what they are called", not "which
    thread a task lands on": ``notify`` wakes whichever worker has been waiting first, so the task
    ownership is itself racy.
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
    # once core is reached no more threads are created and a task is taken from the queue by an existing worker
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
    """Callback delivery: when the pool is unavailable (shut down / rejected) it does **runInThisThread**, the callback must not get lost."""
    p = _producer()
    pool = p._callback_executor
    pool.shutdown(wait=True)
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 3000)
    _wait_done(cb)
    assert cb.results
    assert cb.threads and not cb.threads[0].startswith("NettyClientPublicExecutor_")


# ---------------------------------------------------------------- asynchronous send back pressure
# Asynchronous sending has two gates: a count gate and a byte gate, and the permits are returned in
# the callback. It is off by default, so none of the test cases above had to be changed.
# The floor value of the byte gate is 1M, so here the assertion can only be phrased as "how much less than
# 1M"; the upper limit cannot be configured to a few hundred bytes -- that would be clamped back to 1M.
def _backpressure_producer(num: int = 10, size: int = MIN_ASYNC_SEND_SIZE,
                           **kwargs) -> DefaultMQProducer:
    """A producer with back pressure on. The byte gate is clamped to the floor value 1M: enough to work with, and it makes "how many bytes were deducted" calculable."""
    p = _producer(**kwargs)
    p.set_enable_backpressure_for_async_mode(True)
    p.set_back_pressure_for_async_send_num(num)
    p.set_back_pressure_for_async_send_size(size)
    return p


def test_backpressure_defaults_are_effectively_open():
    """Back pressure is off by default: 1024 entries / 100M bytes."""
    p = DefaultMQProducer("GID_async_test")
    assert p.is_enable_backpressure_for_async_mode() is False
    assert p.get_back_pressure_for_async_send_num() == 1024
    assert p.get_back_pressure_for_async_send_size() == 100 * 1024 * 1024
    assert p.get_semaphore_async_send_num_available_permits() == 1024
    assert p.get_semaphore_async_send_size_available_permits() == 100 * 1024 * 1024
    # while it is off a send neither deducts nor returns any permit
    p._create_async_executors()
    p._mq_client = _FakeAsyncClient([_mq("broker-a", 0)], None, False, None, None, ())
    p._started = True
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 5000), cb, 3000)
    _wait_done(cb)
    assert p.get_semaphore_async_send_num_available_permits() == 1024
    assert p.get_semaphore_async_send_size_available_permits() == 100 * 1024 * 1024


def test_backpressure_num_floor_is_ten_and_size_floor_is_one_meg():
    """A count floor of 10 and a byte floor of 1M -- both lower bounds hold."""
    p = DefaultMQProducer("GID_async_test")
    p.set_back_pressure_for_async_send_num(1)
    p.set_back_pressure_for_async_send_size(1024)
    assert p.get_back_pressure_for_async_send_num() == MIN_ASYNC_SEND_NUM
    assert p.get_back_pressure_for_async_send_size() == MIN_ASYNC_SEND_SIZE
    assert p.get_semaphore_async_send_num_available_permits() == MIN_ASYNC_SEND_NUM
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_num_gate_fails_with_the_protocol_message_and_sends_nothing():
    """When the count permit cannot be obtained it calls back and sends not a single request (the gate itself eats up the budget)."""
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    assert p._semaphore_async_send_num.try_acquire(10, 0) is True   # occupies all the in-flight permits
    began = time.monotonic()
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x"), cb, 300)
    _wait_done(cb)
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert str(cb.errors[0]).endswith("send message tryAcquire semaphoreAsyncNum timeout")
    assert _client(p).attempts_count == 0
    assert int((time.monotonic() - began) * 1000) >= 250, "it handed over the failure without waiting for the timeout"
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE
    p._semaphore_async_send_num.release(10)
    gate.set()


def test_size_gate_fails_with_its_own_message_and_gives_the_num_permit_back():
    """When the byte gate does not pass, the count permits that were **already acquired** must be returned."""
    p = _backpressure_producer(num=10)
    assert p._semaphore_async_send_size.try_acquire(MIN_ASYNC_SEND_SIZE, 0) is True
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 10), cb, 200)
    _wait_done(cb)
    assert isinstance(cb.errors[0], RemotingTooMuchRequestException)
    assert "send message tryAcquire semaphoreAsyncSize timeout" in str(cb.errors[0])
    assert p.get_semaphore_async_send_num_available_permits() == 10, "some count permits were leaked"
    assert _client(p).attempts_count == 0
    p._semaphore_async_send_size.release(MIN_ASYNC_SEND_SIZE)


def test_permits_are_borrowed_per_in_flight_send_and_given_back():
    """One in-flight send = 1 count permit + body.length byte permits; after the callback both come back."""
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
    pass
    assert producer_module._back_pressure_msg_len(Message("T", b"")) == 1
    no_body = Message("T", b"x")
    no_body.body = None
    assert producer_module._back_pressure_msg_len(no_body) == 1
    # A batch accumulates per entry, and the entry with an empty body still counts as 1
    assert producer_module._back_pressure_msg_len(
        [Message("T", b"a" * 3), Message("T", b"")]) == 4


def test_failure_also_gives_the_permits_back():
    pass
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
    """There is only one permit on the retry chain (one send is one entry) and it is returned once at the end; returning it more often inflates the capacity."""
    p = _backpressure_producer(
        num=10, outcomes=[_Deliver(error=RemotingTimeoutException(ADDR_A, 1)),
                          _Deliver(error=RemotingTimeoutException(ADDR_B, 1)),
                          _Deliver(error=RemotingTimeoutException(ADDR_A, 1))])
    cb = _Callback()
    p.send_async(Message("TopicTest", b"x" * 300), cb, 3000)
    _wait_done(cb)
    assert _client(p).attempts_count == 3, "this chain should have terminated after two retries"
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE


def test_gate_waits_for_a_permit_instead_of_failing_early():
    """The semaphore **waits** until the timeout instead of reporting an error as soon as it sees there are not enough -- that is what makes back-pressure rate limiting meaningful."""
    gate = threading.Event()
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
    assert t.is_alive(), "the caller must not return before the permits come back"
    gate.set()
    t.join(6.0)
    assert not t.is_alive()
    assert int((time.monotonic() - began) * 1000) >= 150
    gate.set()
    for cb in held:
        _wait_done(cb)
    _wait_done(third)
    assert third.results, "once the permit is obtained this entry must go out"
    assert p.get_semaphore_async_send_num_available_permits() == MIN_ASYNC_SEND_NUM


def test_runtime_resize_keeps_the_in_flight_share():
    pass
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
    pass
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
    p.set_back_pressure_for_async_send_num(11)     # one extra unit of capacity out of thin air
    t.join(6.0)
    assert not t.is_alive(), "the caller is still stuck at the gate after the capacity was raised"
    gate.set()
    _wait_done(blocked)
    assert blocked.results


def test_queue_full_runs_inline_when_backpressure_is_on():
    pass
    gate = threading.Event()
    p = _backpressure_producer(num=10, block=gate)
    p._async_sender_executor = ConsumeExecutor(1, 1, thread_name_prefix="AsyncSenderExecutor",
                                              max_queue_size=1, thread_name_sep="_",
                                              thread_index_from=1)
    running = _Callback()
    p.send_async(Message("TopicTest", b"1"), running, 8000)
    _wait_attempts(_client(p), 1)
    p.send_async(Message("TopicTest", b"2"), _Callback(), 8000)     # goes into the queue
    for _ in range(200):
        if p._async_sender_executor.queued_count() == 1:
            break
        time.sleep(0.005)
    assert p._async_sender_executor.queued_count() == 1
    # the third entry: the pool rejects it -> it runs in place, occupying the caller until the gate opens
    inline = _Callback()
    returned = threading.Event()

    def _third():
        p.send_async(Message("TopicTest", b"3"), inline, 8000)
        returned.set()

    t = threading.Thread(target=_third)
    t.start()
    time.sleep(0.2)
    assert not returned.is_set(), "running in place should occupy the caller"
    gate.set()
    t.join(6.0)
    assert returned.is_set(), "a full queue must not throw the exception at the caller"
    _wait_done(running)
    _wait_done(inline)
    assert _client(p).attempts_count >= 3
    # nobody waits for the callback of the second entry and the permit is returned on a worker thread:
    # poll instead of asserting right away
    _wait_num_permits(p, 10)


def test_batch_async_charges_the_sum_of_body_lengths():
    pass
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
    pass
    p = _backpressure_producer(num=10)
    client = _client(p)
    client.client_id = "fake-client-id"
    delivered = {}

    def _capture(addr, request, msg, mq, timeout, on_complete):
        delivered["opaque"] = request.opaque
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    client.send_message_async = _capture      # type: ignore[assignment]
    with pytest.raises(RequestTimeoutException) as ei:
        p.request(Message("TopicTest", b"q" * 30), 300)
    assert ei.value.response_code == ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION
    assert "wait reply message timeout" in str(ei.value)
    assert delivered, "request() should go through the asynchronous chain"
    assert p.get_semaphore_async_send_num_available_permits() == 10
    assert p.get_semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE
