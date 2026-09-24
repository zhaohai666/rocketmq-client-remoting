# -*- coding: utf-8 -*-
"""同步发送重试链（对应 Java DefaultMQProducerImpl.sendDefaultImpl）的单测。

对齐基准是 Java 5.5.1 的 ``sendDefaultImpl``，逐条盯这些语义：
  * 路由只在重试循环**之外**取一次；取不到立刻抛 NOT_FOUND_TOPIC_EXCEPTION(10005)；
  * timesTotal = 1 + retryTimesWhenSendFailed，times > 0 时 resetIndex=true；
  * costTime > timeout 时不再发起请求，最终抛 RemotingTooMuchRequestException；
  * sendMsgMaxTimeoutPerRequest 只压**还能再试**的那几次；
  * 异常按类型分档写容错表：MQClientException 只记延迟不隔离，RemotingException
    隔离但保持可达，MQBrokerException 隔离且标不可达；
  * 只有 retryResponseCodes 里的 broker 响应码才换 broker，否则原样抛出
    （已有 SendResult 时把它返回，而不是把错误吞掉）；
  * 非 SEND_OK 只在 retryAnotherBrokerWhenNotStoreOK 打开时才重试；
  * 重试耗尽后按最后异常类型映射 ClientErrorCode：10001 连不上 / 10002 等响应超时
    / 10003 客户端自身问题 / broker 码原样带上。

cpp 端用的是真 socket mock（`cpp/tests/test_send_retry.cpp`），这里走
`_FakeClient` 缝在 `send_message` 上，语义同一套。
"""
from __future__ import annotations

import time
from typing import List, Optional

import pytest

from rocketmq.client.exception import (ClientErrorCode, MQBrokerException,
                                       MQClientException)
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.exception import (RemotingConnectException, RemotingException,
                                         RemotingTimeoutException,
                                         RemotingTooMuchRequestException)
from rocketmq.remoting.protocol.codes import ResponseCode

ADDR = "127.0.0.1:10911"


def _mq(broker: str, queue_id: int = 0) -> MessageQueue:
    return MessageQueue("TopicTest", broker, queue_id)


class _FakeClient:
    """MQClientInstance 替身：按脚本回放 SendResult / 异常，并记录每次的超时预算。"""

    def __init__(self, queues: List[MessageQueue], outcomes: Optional[list] = None,
                 no_route: bool = False, first_send_sleep_ms: int = 0,
                 namesrv_addrs: Optional[List[str]] = None):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = list(queues)
        self.outcomes = list(outcomes or [])
        self.no_route = no_route
        # MQClientInstance 真有这个字段（动态取址时它才是权威来源），
        # validateNameServerSetting 读的就是它，替身少了会 AttributeError。
        self.name_server_addrs = (["127.0.0.1:9876"] if namesrv_addrs is None
                                  else list(namesrv_addrs))
        self.first_send_sleep_ms = first_send_sleep_ms
        self.sent: List[tuple] = []      # (mq, timeout)

    # --- send() 会碰到的 MQClientInstance 接口 ---
    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        if self.no_route:
            raise MQClientException("No route info of this topic: %s" % topic)
        if not self.publish.ok():
            raise MQClientException("Can not find Message Queue for topic: %s" % topic)
        return self.publish

    def broker_addr_of(self, broker_name):
        return ADDR

    def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                     default_topic=None, default_topic_queue_nums=None):
        self.sent.append((mq, timeout))
        if self.first_send_sleep_ms and len(self.sent) == 1:
            time.sleep(self.first_send_sleep_ms / 1000.0)
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)

    # --- 断言用的便捷属性 ---
    @property
    def sends(self) -> int:
        return len(self.sent)

    @property
    def brokers(self) -> List[str]:
        return [mq.broker_name for mq, _ in self.sent]

    @property
    def timeouts(self) -> List[int]:
        return [timeout for _, timeout in self.sent]


def _producer(queues=None, outcomes=None, retry=2, no_route=False,
              first_send_sleep_ms=0, latency_fault=False, max_per_request=None,
              retry_not_store_ok=None, namesrv_addrs=None) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(retry)
    if latency_fault:
        p.set_send_latency_fault_enable(True)
    if max_per_request is not None:
        p.set_send_msg_max_timeout_per_request(max_per_request)
    if retry_not_store_ok is not None:
        p.set_retry_another_broker_when_not_store_ok(retry_not_store_ok)
    p._mq_client = _FakeClient([_mq("broker-a")] if queues is None else list(queues),
                               outcomes, no_route, first_send_sleep_ms,
                               namesrv_addrs=namesrv_addrs)
    p._started = True
    return p


def _client(p: DefaultMQProducer) -> _FakeClient:
    return p._mq_client


def _ok(mq):
    return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)


# ---------------------------------------------------------------- 路由
def test_missing_route_fails_fast_with_not_found_topic_code():
    """Java：tryToFindTopicPublishInfo 拿不到 → 直接 10005，一次请求都不发。"""
    p = _producer(outcomes=[MQClientException("boom")], retry=3, no_route=True)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION
    assert _client(p).sends == 0


def test_empty_publish_info_also_maps_to_not_found_topic():
    p = _producer(retry=2, queues=[])
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION
    assert _client(p).sends == 0


def test_no_name_server_address_reports_10004_not_missing_route():
    """Java ``validateNameServerSetting``（DefaultMQProducerImpl:729）：一个地址都没有时报
    10004，而不是把寻址故障说成"这个 topic 没路由"（10005）。

    少了这一步，配错地址服务器的运维会去查 topic 存在不存在，方向完全错。
    """
    p = _producer(no_route=True, retry=2, namesrv_addrs=[])
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.NO_NAME_SERVER_EXCEPTION
    assert str(ei.value) == "No name server address, please set it."
    assert _client(p).sends == 0


def test_name_server_configured_keeps_the_10005_no_route_code():
    """有地址、只是这个 topic 没路由 → 10004 不能把 10005 顶掉（同一条检查的对照分支）。"""
    p = _producer(no_route=True, retry=2, namesrv_addrs=["127.0.0.1:9876"])
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION


# ---------------------------------------------------------------- 错误码表
def test_client_error_code_table_matches_java():
    """``client/src/main/java/org/apache/rocketmq/client/common/ClientErrorCode.java``
    一共七个常量，一个都不能少、一个都不能改值。

    10001~10005 是发送重试的定性；10006/10007 各有各的抛出点（request-reply 超时、
    造应答消息失败），以前表里缺这两个，站点只能拿默认码 1/None 抛出去。
    """
    expected = {
        "CONNECT_BROKER_EXCEPTION": 10001,
        "ACCESS_BROKER_TIMEOUT": 10002,
        "BROKER_NOT_EXIST_EXCEPTION": 10003,
        "NO_NAME_SERVER_EXCEPTION": 10004,
        "NOT_FOUND_TOPIC_EXCEPTION": 10005,
        "REQUEST_TIMEOUT_EXCEPTION": 10006,
        "CREATE_REPLY_MESSAGE_EXCEPTION": 10007,
    }
    got = {k: v for k, v in vars(ClientErrorCode).items() if k.isupper()}
    assert got == expected


# ---------------------------------------------------------------- 尝试次数
def test_send_ok_returns_without_consuming_retries():
    p = _producer(retry=3)
    result = p.send(Message("TopicTest", b"x"))
    assert result.send_status == SendStatus.SEND_OK
    assert _client(p).sends == 1


def test_retry_times_plus_one_attempts_when_everything_fails():
    """retryTimesWhenSendFailed=2 → 一共 3 次尝试，异常码是最后一次的 broker 码。"""
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR, "s"),
                            MQBrokerException(ResponseCode.SYSTEM_ERROR, "s"),
                            MQBrokerException(ResponseCode.SYSTEM_ERROR, "s")],
                  retry=2)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert _client(p).sends == 3
    assert ei.value.response_code == ResponseCode.SYSTEM_ERROR
    assert "Send [3] times, still failed" in str(ei.value)


def test_retry_zero_sends_exactly_once():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)], retry=0)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"))
    assert _client(p).sends == 1


def test_success_after_two_failures_returns_result():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR),
                            RemotingTimeoutException(ADDR, 3000),
                            _ok(_mq("broker-a"))], retry=2)
    result = p.send(Message("TopicTest", b"x"))
    assert result.send_status == SendStatus.SEND_OK
    assert _client(p).sends == 3


# ---------------------------------------------------------------- retryResponseCodes
def test_non_retryable_broker_code_raises_immediately():
    """MESSAGE_ILLEGAL 不在 retryResponseCodes 里：重试也是白试，必须原样抛出。"""
    p = _producer(outcomes=[MQBrokerException(ResponseCode.MESSAGE_ILLEGAL, "bad"),
                            _ok(_mq("broker-a"))], retry=2)
    with pytest.raises(MQBrokerException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ResponseCode.MESSAGE_ILLEGAL
    assert _client(p).sends == 1


def test_all_java_default_retry_response_codes_are_retryable():
    expected = {1, 2, 14, 16, 17, 204, 205, 1500}
    p = DefaultMQProducer("GID_test")
    assert set(p.retry_response_codes) == expected
    for code in expected:
        assert p.is_retry_response_code(code), code


def test_custom_retry_response_code_is_honoured():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.CONSUMER_NOT_ONLINE),
                            _ok(_mq("broker-a"))], retry=2)
    assert p.is_retry_response_code(ResponseCode.CONSUMER_NOT_ONLINE) is False
    p.add_retry_response_code(ResponseCode.CONSUMER_NOT_ONLINE)
    assert p.send(Message("TopicTest", b"x")).send_status == SendStatus.SEND_OK
    assert _client(p).sends == 2


def test_non_retryable_failure_returns_earlier_send_result():
    """Java：先拿到非 SEND_OK 的结果，后一轮抛了不可重试的码 → 返回那个结果，不吞成功。"""
    p = _producer(outcomes=[SendResult(SendStatus.FLUSH_SLAVE_TIMEOUT, msg_id="1" * 32),
                            MQBrokerException(ResponseCode.MESSAGE_ILLEGAL)],
                  retry=2, retry_not_store_ok=True)
    result = p.send(Message("TopicTest", b"x"))
    assert result.send_status == SendStatus.FLUSH_SLAVE_TIMEOUT
    assert _client(p).sends == 2


# ---------------------------------------------------------------- 非 SEND_OK
def test_not_store_ok_returned_as_is_by_default():
    """Java 默认 retryAnotherBrokerWhenNotStoreOK=false：存了但没存好也原样返回。"""
    p = _producer(outcomes=[SendResult(SendStatus.FLUSH_DISK_TIMEOUT, msg_id="2" * 32)],
                  retry=2)
    result = p.send(Message("TopicTest", b"x"))
    assert result.send_status == SendStatus.FLUSH_DISK_TIMEOUT
    assert _client(p).sends == 1


def test_not_store_ok_retries_another_broker_when_enabled():
    p = _producer(outcomes=[SendResult(SendStatus.SLAVE_NOT_AVAILABLE, msg_id="3" * 32),
                            SendResult(SendStatus.SLAVE_NOT_AVAILABLE, msg_id="3" * 32)],
                  retry=1, retry_not_store_ok=True)
    result = p.send(Message("TopicTest", b"x"))
    assert result.send_status == SendStatus.SLAVE_NOT_AVAILABLE
    assert _client(p).sends == 2
    assert p.is_retry_another_broker_when_not_store_ok() is True


# ---------------------------------------------------------------- 超时预算
def test_send_msg_max_timeout_per_request_only_caps_retriable_attempts():
    """Java：maxTimeoutPerRequest 只在「还能再试」时压缩 curTimeout，最后一次给满预算。"""
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)] * 3,
                  retry=2, max_per_request=100)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"), timeout_millis=3000)
    timeouts = _client(p).timeouts
    assert len(timeouts) == 3
    assert timeouts[0] == 100 and timeouts[1] == 100
    assert timeouts[2] > 2900, "最后一次不再压缩"


def test_without_cap_every_attempt_gets_full_remaining_budget():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)] * 3, retry=2)
    assert p.get_send_msg_max_timeout_per_request() == -1
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"), timeout_millis=3000)
    timeouts = _client(p).timeouts
    assert all(t > 2900 for t in timeouts), timeouts


def test_call_timeout_stops_retrying_and_raises_too_much_request():
    """首轮就把总预算吃光 → 不再发第二轮，抛 RemotingTooMuchRequestException。"""
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)], retry=2,
                  first_send_sleep_ms=60)
    with pytest.raises(RemotingTooMuchRequestException) as ei:
        p.send(Message("TopicTest", b"x"), timeout_millis=30)
    assert "call timeout" in str(ei.value)
    assert _client(p).sends == 1


def test_existing_result_wins_over_call_timeout():
    """Java 顺序是先 `if (sendResult != null) return`，再判 callTimeout。"""
    p = _producer(outcomes=[SendResult(SendStatus.FLUSH_DISK_TIMEOUT, msg_id="4" * 32),
                            MQBrokerException(ResponseCode.SYSTEM_ERROR)],
                  retry=2, retry_not_store_ok=True, first_send_sleep_ms=60)
    result = p.send(Message("TopicTest", b"x"), timeout_millis=30)
    assert result.send_status == SendStatus.FLUSH_DISK_TIMEOUT


# ---------------------------------------------------------------- 队列选择
def test_retry_resets_index_and_switches_broker():
    p = _producer(queues=[_mq("broker-a"), _mq("broker-b")],
                  outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR),
                            MQBrokerException(ResponseCode.SYSTEM_ERROR),
                            _ok(_mq("broker-a"))], retry=2)
    p.send(Message("TopicTest", b"x"))
    brokers = _client(p).brokers
    assert brokers[0] != brokers[1], "重试要换到别的 broker（lastBrokerName + resetIndex）"
    assert len(set(brokers[:2])) == 2


# ---------------------------------------------------------------- 错误码映射
def test_connect_failure_maps_to_connect_broker_exception():
    p = _producer(outcomes=[RemotingConnectException(ADDR)], retry=0)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.CONNECT_BROKER_EXCEPTION


def test_wait_response_timeout_maps_to_access_broker_timeout():
    p = _producer(outcomes=[RemotingTimeoutException(ADDR, 3000)], retry=0)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.ACCESS_BROKER_TIMEOUT


def test_client_failure_maps_to_broker_not_exist_exception():
    p = _producer(outcomes=[MQClientException("no queue")], retry=0)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code == ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION


def test_plain_remoting_failure_leaves_response_code_unset():
    """既不是连不上也不是等响应超时 → Java 不套任何 ClientErrorCode，code 留空。"""
    p = _producer(outcomes=[RemotingException("network")], retry=0)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert ei.value.response_code is None
    assert isinstance(ei.value.cause, RemotingException)


def test_unexpected_exception_propagates_unchanged():
    """Java 只声明四类发送异常；其余异常不重试、不包装，原样抛出。"""
    p = _producer(outcomes=[ValueError("unexpected"), _ok(_mq("broker-a"))], retry=2)
    with pytest.raises(ValueError):
        p.send(Message("TopicTest", b"x"))
    assert _client(p).sends == 1


def test_last_error_kept_as_cause():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_BUSY, "busy")] * 2,
                  retry=1)
    with pytest.raises(MQClientException) as ei:
        p.send(Message("TopicTest", b"x"))
    assert isinstance(ei.value.cause, MQBrokerException)
    assert "broker-a" in str(ei.value)


# ---------------------------------------------------------------- 容错表
def _fault(p, broker="broker-a"):
    return p._mq_fault_strategy.latency_fault_tolerance.get_fault_item(broker)


def test_successful_send_records_positive_latency_without_isolation():
    p = _producer(latency_fault=True)
    p.send(Message("TopicTest", b"x"))
    item = _fault(p)
    assert item is not None
    assert item.is_available() and item.is_reachable()
    # 亚毫秒往返用墙钟毫秒差会记成 0，那样延迟阈值永远不生效
    assert item.current_latency > 0.0, "latency must be measured on a monotonic clock"


def test_client_exception_records_latency_without_isolation():
    """Java catch (MQClientException) → updateFaultItem(..., false, true)：只记延迟。"""
    p = _producer(outcomes=[MQClientException("x")] * 2, retry=1, latency_fault=True)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"))
    item = _fault(p)
    assert item.is_available(), "客户端自身问题不该把 broker 隔离"
    assert item.is_reachable()


def test_remoting_exception_isolates_but_keeps_reachable():
    """无后台探测线程 → Java 的 reachable = !isStartDetectorEnable() 恒为 true。"""
    p = _producer(outcomes=[RemotingConnectException(ADDR)] * 2, retry=1, latency_fault=True)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"))
    item = _fault(p)
    assert not item.is_available(), "RemotingException 要隔离该 broker"
    assert item.is_reachable()


def test_broker_exception_isolates_and_marks_unreachable():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)] * 2,
                  retry=1, latency_fault=True)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"))
    item = _fault(p)
    assert not item.is_available()
    assert item.is_reachable() is False


def test_fault_table_untouched_when_latency_fault_disabled():
    p = _producer(outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR)] * 2, retry=1)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"x"))
    assert _fault(p) is None


def test_isolated_broker_is_avoided_on_next_send():
    """一次 SYSTEM_ERROR 把 broker-a 隔离；下次发送必须避开它。"""
    queues = [_mq("broker-a"), _mq("broker-b")]
    p = _producer(queues=queues, outcomes=[MQBrokerException(ResponseCode.SYSTEM_ERROR),
                                           _ok(_mq("broker-b"))],
                  retry=1, latency_fault=True)
    p.send(Message("TopicTest", b"x"))
    assert _client(p).brokers[1] == "broker-b"
    assert not _fault(p, "broker-a").is_available()
    assert _fault(p, "broker-b").is_available()
