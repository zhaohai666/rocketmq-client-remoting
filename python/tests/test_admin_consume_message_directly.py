# -*- coding: utf-8 -*-
from __future__ import annotations

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from common.message_const import MessageConst
from common.message import MessageExt
from remoting.protocol.body import CMResult, ConsumeMessageDirectlyResult
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_AdminDirectlyUnit"
TOPIC = "AdminDirectlyUnitTopic"
CLIENT_ID = "10.0.0.7@CONSUMER"
STORE_ADDR = "127.0.0.1:10911"
OFFSET_MSG_ID = "0A00000100002A0000000000000016B0"
UNIQ_MSG_ID = "C0A80164123456780000000000001234"


# ------------------------------------------------------------------ Test doubles

def _ok(body: bytes = b"") -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    return r


def _err(code: int, remark: str) -> RemotingCommand:
    r = RemotingCommand()
    r.code = code
    r.remark = remark
    return r


class _FakeClient:
    """Replace the admin's synchronous network layer and route lookups, collecting outgoing requests."""

    def __init__(self, responses, route=None):
        self._responses = list(responses)
        self.requests = []  # [(addr, RemotingCommand)]
        self.route = route
        self.route_lookups = []  # [topic]

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request))
        if not self._responses:
            raise AssertionError("unexpected extra request: %s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get_topic_route_data(self, topic, timeout_millis=None):
        self.route_lookups.append(topic)
        return self.route

    def _check_response(self, response):
        """Same semantics as `MQClientInstance._check_response:524`: raise MQBrokerException on non-SUCCESS."""
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")


def _admin(client: _FakeClient, msg: MessageExt) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    admin.view_message = lambda topic, msg_id: msg  # noqa: GTW
    return admin


def _msg(uniq_key: bool = True) -> MessageExt:
    m = MessageExt(TOPIC, b"hello")
    m.store_host = "127.0.0.1"
    m.store_host_port = 10911
    m.offset_msg_id = OFFSET_MSG_ID
    if uniq_key:
        m.put_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, UNIQ_MSG_ID)
    return m


def _result_body(consume_result: str = CMResult.CR_SUCCESS,
                 remark: str = "OK") -> bytes:
    r = ConsumeMessageDirectlyResult()
    r.order = False
    r.auto_commit = True
    r.consume_result = consume_result
    r.remark = remark
    r.spent_time_mills = 12
    return r.encode()


def _route(*broker_names_and_addrs) -> TopicRouteData:
    route = TopicRouteData()
    for name, addr in broker_names_and_addrs:
        route.broker_datas.append(BrokerData("DefaultCluster", name, {0: addr}))
    return route


# ------------------------------------------------------------------ Header shape

def test_uniq_key_msg_id_is_replaced_by_offset_msg_id():
    """The client must replace uniqKey with offsetMsgId before sending: the broker fetches the message by commitLog offset."""
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=True))

    result = admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)

    assert len(client.requests) == 1
    addr, request = client.requests[0]
    # Sent to the broker storing this message (storeHost resolved by viewMessage)
    assert addr == STORE_ADDR
    assert request.code == RequestCode.CONSUME_MESSAGE_DIRECTLY
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID
    assert request.ext_fields["consumerGroup"] == GROUP
    assert request.ext_fields["clientId"] == CLIENT_ID
    assert request.ext_fields["topic"] == TOPIC
    # The response body is decided by the consumer side; decode it as-is
    assert result.consume_result == CMResult.CR_SUCCESS
    assert result.remark == "OK"
    assert result.spent_time_mills == 12
    assert result.auto_commit is True


def test_plain_offset_msg_id_is_forwarded_unchanged():
    pass
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID


def test_header_omits_absent_optional_fields():
    pass
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert "brokerName" not in request.ext_fields
    assert "topicSysFlag" not in request.ext_fields
    assert "groupSysFlag" not in request.ext_fields


def test_broker_name_enriched_from_route_when_addr_matches():
    pass
    client = _FakeClient([_ok(_result_body())],
                         route=_route(("broker-a", STORE_ADDR),
                                      ("broker-b", "127.0.0.1:10921")))
    admin = _admin(client, _msg(uniq_key=True))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)

    addr, request = client.requests[0]
    assert addr == STORE_ADDR
    assert request.ext_fields["brokerName"] == "broker-a"


def test_route_lookup_failure_does_not_block_the_call():
    """Route lookup failure (network flapping / no permission) only affects brokerName enrichment; the main flow proceeds."""
    client = _FakeClient([_ok(_result_body())], route=None)
    admin = _admin(client, _msg(uniq_key=False))

    result = admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID
    assert result.consume_result == CMResult.CR_SUCCESS


def test_explicit_broker_addr_overrides_store_host():
    """Explicit brokerAddr takes precedence over the storeHost resolved by viewMessage (Address-form override parameter)."""
    other = "10.1.1.5:10911"
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID,
                                   broker_addr=other)

    addr, _ = client.requests[0]
    assert addr == other


# ------------------------------------------------------------------ Error contract

def test_empty_body_raises_client_exception():
    """A broker SUCCESS with an empty body (when the message lookup is swallowed) must fail explicitly instead of returning an empty result with consumeResult=None."""
    client = _FakeClient([_ok(b"")])
    admin = _admin(client, _msg(uniq_key=True))

    try:
        admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)
        raise AssertionError("expected MQClientException")
    except MQClientException as e:
        assert "no consume result" in str(e)


def test_broker_rejection_raises_broker_exception():
    """Non-SUCCESS follows the `_check_response` MQBrokerException contract (e.g. group not online)."""
    client = _FakeClient([_err(ResponseCode.CONSUMER_NOT_ONLINE, "consumer not online")])
    admin = _admin(client, _msg(uniq_key=True))

    try:
        admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)
        raise AssertionError("expected MQBrokerException")
    except MQBrokerException as e:
        assert e.response_code == ResponseCode.CONSUMER_NOT_ONLINE


def test_missing_group_or_client_id_fails_fast():
    """Missing group / clientId fails locally without issuing any network request."""
    client = _FakeClient([])
    admin = _admin(client, _msg(uniq_key=True))

    for kwargs in ({"consumer_group": "", "client_id": CLIENT_ID},
                   {"consumer_group": GROUP, "client_id": ""}):
        try:
            admin.consume_message_directly(kwargs["consumer_group"],
                                           kwargs["client_id"], TOPIC, UNIQ_MSG_ID)
            raise AssertionError("expected MQClientException for %s" % kwargs)
        except MQClientException as e:
            assert "consumerGroup/clientId" in str(e)
    assert client.requests == []
