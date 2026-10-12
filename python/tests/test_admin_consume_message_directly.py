# -*- coding: utf-8 -*-
"""Admin 发起端 consumeMessageDirectly(309) 的离线协议单测，不需要集群。

对齐 Java `DefaultMQAdminExtImpl#consumeMessageDirectly:1323-1332` 与
`MQClientAPIImpl#consumeMessageDirectly:2741`。这条链路有两个「错一点就静默失效」的
口径，用例把它们锁死：

  - msgId 带客户端 UNIQ_KEY 属性时必须改发 **offsetMsgId**：broker 端
    `AdminBrokerProcessor:2629` 拿 msgId 当 commitLog 偏移 `selectOneByOffset`，
    发客户端生成的 uniqKey 必然查空，broker 只能把 body=null 转下去。
  - 头字段名必须与 Java `ConsumeMessageDirectlyResultRequestHeader` 的字段一致
    （consumerGroup/clientId/msgId/topic），brokerName 是可空补充（Java 发起端
    不设，这里路由对得上时才带）。

viewMessage 的两条解析路径（offset msgId 直解 / uniqKey 索引兜底）已有覆盖，
这里按 Java 的分层把 viewMessage 替身化，只测发起端自己的逻辑。
"""
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


# ------------------------------------------------------------------ 测试替身

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
    """替掉 admin 的同步网络层与路由查询，收集发出的请求。"""

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
        """与 `MQClientInstance._check_response:524` 同语义：非 SUCCESS 抛 MQBrokerException。"""
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")


def _admin(client: _FakeClient, msg: MessageExt) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    # Java `DefaultMQAdminExtImpl#consumeMessageDirectly` 第一步就是 viewMessage；
    # 两条解析路径已由 view_message 自己的用例覆盖，这里替身化只测发起端逻辑。
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


# ------------------------------------------------------------------ 头形状

def test_uniq_key_msg_id_is_replaced_by_offset_msg_id():
    """客户端 uniqKey 必须换成 offsetMsgId 再下发：broker 按 commitLog 偏移取消息。"""
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=True))

    result = admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)

    assert len(client.requests) == 1
    addr, request = client.requests[0]
    # 发给存储这条消息的 broker（viewMessage 解析出的 storeHost）
    assert addr == STORE_ADDR
    assert request.code == RequestCode.CONSUME_MESSAGE_DIRECTLY
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID
    assert request.ext_fields["consumerGroup"] == GROUP
    assert request.ext_fields["clientId"] == CLIENT_ID
    assert request.ext_fields["topic"] == TOPIC
    # 应答 body 是消费者端判定，原样解码
    assert result.consume_result == CMResult.CR_SUCCESS
    assert result.remark == "OK"
    assert result.spent_time_mills == 12
    assert result.auto_commit is True


def test_plain_offset_msg_id_is_forwarded_unchanged():
    """消息没有 UNIQ_KEY 属性（msgId 本身就是 offsetMsgId）时原样透传——Java
    `DefaultMQAdminExtImpl:1327-1328` 的 if 分支。"""
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID


def test_header_omits_absent_optional_fields():
    """brokerName / topicSysFlag 是 Java 头里的可空字段，拿不到就**不发**这个键
    （_ext 过滤 None，与 Java makeCustomHeaderToNet 一致），不能落成 "None"。"""
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert "brokerName" not in request.ext_fields
    assert "topicSysFlag" not in request.ext_fields
    assert "groupSysFlag" not in request.ext_fields


def test_broker_name_enriched_from_route_when_addr_matches():
    """路由能对上 storeHost 时补 brokerName（本仓库 309 接收端用它构造 MessageQueue）；
    Java 发起端不设该字段，这里只做尽力而为的补充。"""
    client = _FakeClient([_ok(_result_body())],
                         route=_route(("broker-a", STORE_ADDR),
                                      ("broker-b", "127.0.0.1:10921")))
    admin = _admin(client, _msg(uniq_key=True))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)

    addr, request = client.requests[0]
    assert addr == STORE_ADDR
    assert request.ext_fields["brokerName"] == "broker-a"


def test_route_lookup_failure_does_not_block_the_call():
    """路由查不到（网络抖动/无权限）只影响 brokerName 补充，主流程照发。"""
    client = _FakeClient([_ok(_result_body())], route=None)
    admin = _admin(client, _msg(uniq_key=False))

    result = admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID)

    _, request = client.requests[0]
    assert request.ext_fields["msgId"] == OFFSET_MSG_ID
    assert result.consume_result == CMResult.CR_SUCCESS


def test_explicit_broker_addr_overrides_store_host():
    """显式 brokerAddr 优先于 viewMessage 解析出的 storeHost（Address 形态覆盖参数）。"""
    other = "10.1.1.5:10911"
    client = _FakeClient([_ok(_result_body())])
    admin = _admin(client, _msg(uniq_key=False))

    admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, OFFSET_MSG_ID,
                                   broker_addr=other)

    addr, _ = client.requests[0]
    assert addr == other


# ------------------------------------------------------------------ 错误契约

def test_empty_body_raises_client_exception():
    """broker 回 SUCCESS 但 body 为空（消息查空被吞时）必须显式失败，不能返回
    一个 consumeResult=None 的空结果。"""
    client = _FakeClient([_ok(b"")])
    admin = _admin(client, _msg(uniq_key=True))

    try:
        admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)
        raise AssertionError("expected MQClientException")
    except MQClientException as e:
        assert "no consume result" in str(e)


def test_broker_rejection_raises_broker_exception():
    """非 SUCCESS 走 `_check_response` 的 MQBrokerException 契约（组不在线等）。"""
    client = _FakeClient([_err(ResponseCode.CONSUMER_NOT_ONLINE, "consumer not online")])
    admin = _admin(client, _msg(uniq_key=True))

    try:
        admin.consume_message_directly(GROUP, CLIENT_ID, TOPIC, UNIQ_MSG_ID)
        raise AssertionError("expected MQBrokerException")
    except MQBrokerException as e:
        assert e.response_code == ResponseCode.CONSUMER_NOT_ONLINE


def test_missing_group_or_client_id_fails_fast():
    """group / clientId 缺省直接本地失败，不发起任何网络请求。"""
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
