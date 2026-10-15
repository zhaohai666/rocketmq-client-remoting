# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import List, Optional

import pytest

from client.consumer import DefaultMQPushConsumer
from client.exception import MQClientException
from client.mq_client import MQClientInstance
from common.subscription_data import ExpressionType, SubscriptionData
from remoting.exception import RemotingConnectException
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_CheckCfg"
TOPIC = "Sql92Topic"
BROKER = "127.0.0.1:10911"
CLIENT_ID = "127.0.0.1@1234#5678"


def _sub(topic: str = TOPIC, expression: str = "a > 10",
         expression_type: str = ExpressionType.SQL92) -> SubscriptionData:
    s = SubscriptionData(topic=topic, sub_string=expression)
    s.expression_type = expression_type
    return s


def _consumer(*subs: SubscriptionData) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    c.subscription_data = {s.topic: s for s in subs}
    return c


class _Recorder:
    """Double remoting layer: records every request and replays response codes per script (None means raise)."""

    def __init__(self, responses: Optional[list] = None):
        self.requests: List[tuple] = []          # (addr, RemotingCommand)
        self.responses = list(responses or [])

    def invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request, timeout_millis))
        outcome = self.responses.pop(0) if self.responses else ResponseCode.SUCCESS
        if isinstance(outcome, BaseException):
            raise outcome
        return RemotingCommand.create_response_command(
            outcome, "remark-%d" % outcome if outcome != ResponseCode.SUCCESS else None)

    # MQClientInstance only touches remoting_client inside _invoke_sync; other methods do not
    def register_rpc_hook(self, *a, **kw):
        pass

    def register_processor(self, *a, **kw):
        pass

    def close(self, *a, **kw):
        pass

    @property
    def codes(self) -> List[int]:
        return [req.code for _, req, _ in self.check_requests]

    @property
    def check_requests(self) -> List[tuple]:
        """Only look at request 46: when the route cache is empty, ``find_broker_addr_by_topic`` opportunistically pulls the route once more
        (GET_ROUTEINFO_BY_TOPIC also lands here); that is an expected side effect and must not affect assertions.
        """
        return [(addr, req, t) for addr, req, t in self.requests
                if req.code == RequestCode.CHECK_CLIENT_CONFIG]

    def body_of(self, index: int = 0) -> dict:
        _, req, _ = self.check_requests[index]
        return json.loads(req.body.decode("utf-8"))


def _instance(*subs: SubscriptionData) -> MQClientInstance:
    inst = MQClientInstance(CLIENT_ID, ["127.0.0.1:9876"])
    rec = _Recorder()
    inst.remoting_client = rec
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a", {0: BROKER})]
    for s in subs:
        inst.topic_route_table[s.topic] = route
    if subs:
        inst.register_consumer(GROUP, _consumer(*subs))
    return inst, rec


def test_tag_only_subscription_sends_nothing():
    pass
    inst, rec = _instance(
        _sub("TagTopic", "tagA || tagB", ExpressionType.TAG),
        _sub("NullTypeTopic", "*", None),
        _sub("EmptyTypeTopic", "*", ""),
    )
    inst.check_client_in_broker()
    assert rec.requests == []


def test_sql92_request_shape_matches_the_wire_contract():
    inst, rec = _instance(_sub())
    inst.check_client_in_broker()
    assert rec.codes == [RequestCode.CHECK_CLIENT_CONFIG]
    addr, req, timeout = rec.check_requests[0]
    assert addr == BROKER
    # A null request header means no extFields on the wire; the body is CheckClientRequestBody JSON
    assert not req.ext_fields
    assert timeout == 3000
    body = rec.body_of()
    assert body["clientId"] == CLIENT_ID
    assert body["group"] == GROUP
    sd = body["subscriptionData"]
    assert sd["topic"] == TOPIC
    assert sd["subString"] == "a > 10"
    assert sd["expressionType"] == ExpressionType.SQL92
    assert sorted(sd.keys()) == ["classFilterMode", "codeSet", "expressionType",
                                 "subString", "subVersion", "tagsSet", "topic"]
    assert "filterClassSource" not in sd


def test_broker_reject_code_becomes_client_exception():
    """SUBSCRIPTION_PARSE_FAILED(23) carries the response code as-is — this is the criterion for startup failure."""
    inst, rec = _instance(_sub(expression="a >"))
    rec.responses = [ResponseCode.SUBSCRIPTION_PARSE_FAILED]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    assert ei.value.response_code == ResponseCode.SUBSCRIPTION_PARSE_FAILED
    assert "remark" in str(ei.value)


def test_broker_without_property_filter_support_reports_system_error():
    pass
    inst, rec = _instance(_sub())
    rec.responses = [ResponseCode.SYSTEM_ERROR]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    assert ei.value.response_code == ResponseCode.SYSTEM_ERROR


def test_no_route_skips_the_subscription():
    pass
    inst, rec = _instance(_sub())
    inst.topic_route_table.clear()
    inst.check_client_in_broker()
    assert rec.check_requests == []


def test_transport_error_is_wrapped_with_the_protocol_message():
    pass
    inst, rec = _instance(_sub())
    rec.responses = [RemotingConnectException(BROKER)]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    msg = str(ei.value)
    assert "SQL92" in msg
    assert "server has not been upgraded to support" in msg
    assert isinstance(ei.value.cause, RemotingConnectException)


def test_empty_subscription_returns_whole_check():
    pass
    inst = MQClientInstance(CLIENT_ID, ["127.0.0.1:9876"])
    rec = _Recorder()
    inst.remoting_client = rec
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a", {0: BROKER})]
    inst.topic_route_table[TOPIC] = route
    inst.register_consumer("GID_empty", _consumer())
    inst.register_consumer(GROUP, _consumer(_sub()))
    inst.check_client_in_broker()
    assert rec.requests == []


def test_check_subscriptions_helper_covers_unregistered_consumers():
    """consumerTable only holds push-mode consumers; unregistered callers get the same semantics via the inner loop."""
    inst, rec = _instance(_sub())
    inst.check_subscriptions_in_broker(GROUP, [_sub()])
    assert rec.codes == [RequestCode.CHECK_CLIENT_CONFIG]
    assert rec.body_of()["group"] == GROUP
