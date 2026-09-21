# -*- coding: utf-8 -*-
"""admin 位点重置 / 组级 topic 反查的离线协议单测，不需要集群。

对齐 Java `DefaultMQAdminExtImpl#resetOffsetByQueueId:1827` 与 `#queryTopicsByConsumer:1078`。
这两条都是「看着能跑、字段错一位就静默失效」的接口：

  - `resetOffsetByQueueId` 必须**先 25 后 222**，且 222 带 `queueId` + `offset`。只做第
    1 笔的话 broker 的 `resetOffsetTable`（一次性、下次 pull 由
    `ConsumerOffsetManager#queryThenEraseResetOffset` 取走）根本没被写入，在线消费者照旧
    从内存位点继续拉——Java 真机实测「首笔 pull 回 PULL_OFFSET_MOVED」正是这条链路的证据。
  - `resetOffsetByTimestamp` 反过来**不能**带 `queueId`，且 `offset` 必须是 `-1`（Java 的
    null 口径），否则 broker 的 `resetOffsetInner` 会走单队列分支而不按 timestamp 算位点。
  - 组级 `queryTopicsByConsumer` 路由的是 `%RETRY%<group>`（业务 topic 未必落在同一 broker
    上），合并语义对齐 Java `TopicList.topicList`（`Set<String>`）。

真机行为（5.5.1）由 `verify_admin_live.py` 第 9.5/9.6 节覆盖，这里只把协议形状锁死。
"""
from __future__ import annotations

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.exception import MQBrokerException, MQClientException
from rocketmq.common.message import MessageQueue
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.body import ResetOffsetBody, TopicList
from rocketmq.remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_AdminResetUnit"
TOPIC = "AdminResetUnitTopic"
ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
GROUP_RETRY = MixAll.get_retry_topic(GROUP)


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
    """只替掉 admin 用到的三个入口：同步调用、写位点、查路由。"""

    def __init__(self, responses, route=None):
        self._responses = list(responses)
        self.requests = []        # [(addr, RemotingCommand)]
        self.offset_updates = []  # [(group, mq, offset, addr)]
        self.route_lookups = []   # [topic]
        self.route = route

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request))
        if not self._responses:
            raise AssertionError("unexpected extra request: %s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def update_consumer_offset(self, consumer_group, mq, offset,
                               timeout_millis=5000, addr=None):
        self.offset_updates.append((consumer_group, mq, offset, addr))

    def get_topic_route_data(self, topic, timeout_millis=None):
        self.route_lookups.append(topic)
        return self.route

    def _check_response(self, response):
        """与 `MQClientInstance._check_response:524` 同语义：非 SUCCESS 抛 MQBrokerException。"""
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    # 不起线程、不连集群：只把 client 塞进去，让被测方法走到协议编码那一层
    admin._mq_client = client
    admin._started = True
    return admin


def _route(*broker_names_and_addrs) -> TopicRouteData:
    route = TopicRouteData()
    for name, addr in broker_names_and_addrs:
        route.broker_datas.append(BrokerData("DefaultCluster", name, {0: addr}))
    return route


def _reset_body(mq: MessageQueue, offset: int) -> bytes:
    body = ResetOffsetBody()
    body.offset_table[mq] = offset
    return body.encode()


# ------------------------------------------------------------------ 222 编码

def test_reset_offset_by_queue_id_sends_update_then_222_with_queue_id():
    """Java `DefaultMQAdminExtImpl:1829-1837`：第 1 笔 updateConsumerOffset(25)，第 2 笔带
    queueId + offset 的 222；且单队列重载的 timestamp 传 0、force 不传（Java 默认 false）。
    """
    target_mq = MessageQueue(TOPIC, "", 3)
    # 第 1 笔(25)在 client.update_consumer_offset 这一层被替身吃掉，队列里只剩 222 的响应
    client = _FakeClient([_ok(_reset_body(MessageQueue(TOPIC, "broker-a", 3), 7))])
    admin = _admin(client)

    result = admin.reset_offset_by_queue_id(ADDR_A, GROUP, TOPIC, 3, 7)

    # 第 1 笔：25 由 MQClientInstance 发出，且必须显式打在给定 broker 上
    assert client.offset_updates == [(GROUP, target_mq, 7, ADDR_A)]
    # 第 2 笔：222
    assert len(client.requests) == 1
    addr, request = client.requests[0]
    assert addr == ADDR_A
    assert request.code == RequestCode.INVOKE_BROKER_TO_RESET_OFFSET
    assert request.ext_fields["queueId"] == "3"
    assert request.ext_fields["offset"] == "7"
    assert request.ext_fields["timestamp"] == "0"
    assert request.ext_fields["force"] == "false"
    assert request.ext_fields["topic"] == TOPIC
    assert request.ext_fields["group"] == GROUP
    assert result == {MessageQueue(TOPIC, "broker-a", 3): 7}


def test_reset_offset_by_timestamp_omits_queue_id_and_forces_cpp_language():
    """按 timestamp 的那笔必须**不带** queueId、offset 写成 -1（Java 的 null 口径），
    并把 language 覆盖成 CPP——broker 只对 CPP/PYTHON 回可解析的 offsetTable。
    """
    client = _FakeClient([_ok(_reset_body(MessageQueue(TOPIC, "broker-a", 0), 9))],
                         route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)

    offsets = admin.reset_offset_by_timestamp(TOPIC, GROUP, 1234567890, True)

    addr, request = client.requests[0]
    assert addr == ADDR_A
    assert "queueId" not in request.ext_fields
    assert request.ext_fields["offset"] == "-1"
    assert request.ext_fields["timestamp"] == "1234567890"
    assert request.ext_fields["force"] == "true"
    assert request.language == LanguageCode.CPP
    assert offsets and list(offsets.values()) == [9]
    # 路由按业务 topic 查（LMQ/wheel_timer 才换成 clusterName）
    assert client.route_lookups == [TOPIC]


def test_reset_offset_by_queue_id_propagates_broker_remark():
    """越界位点由 broker 的 `resetOffsetInner` 拒绝（真机 remark：
    `Target offset 102 not in consume queue range [0-2]`）；必须原样带上 code+remark，
    而不是吞成"重置成功但空表"。
    """
    client = _FakeClient([
        _err(ResponseCode.SYSTEM_ERROR, "Target offset 102 not in consume queue range [0-2]"),
    ])
    admin = _admin(client)

    try:
        admin.reset_offset_by_queue_id(ADDR_A, GROUP, TOPIC, 0, 102)
        raise AssertionError("expected MQClientException")
    except MQClientException as e:
        assert e.response_code == ResponseCode.SYSTEM_ERROR
        assert "not in consume queue range" in str(e)
    # 第 1 笔已经把非法位点落库：Java 语义（两笔 RPC 非原子），这里不做保护性回滚
    assert len(client.offset_updates) == 1


def test_reset_offset_by_timestamp_requires_at_least_one_offset_table():
    """所有 broker 都回空表时按 Java 抛错，避免「静默重置失败」。"""
    client = _FakeClient([_ok(b"")], route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)
    try:
        admin.reset_offset_by_timestamp(TOPIC, GROUP, 1, True)
        raise AssertionError("expected MQClientException")
    except MQClientException:
        pass


# ---------------------------------------------------------- 343 queryTopicsByConsumer

def _topic_list_body(topics) -> bytes:
    tl = TopicList()
    tl.topic_list = list(topics)
    tl.broker_addr = ADDR_A
    return tl.encode()


def test_query_topics_by_consumer_to_broker_uses_343_with_group():
    client = _FakeClient([_ok(_topic_list_body([TOPIC, GROUP_RETRY]))])
    admin = _admin(client)

    result = admin.query_topics_by_consumer_to_broker(ADDR_A, GROUP)

    addr, request = client.requests[0]
    assert (addr, request.code) == (ADDR_A, RequestCode.QUERY_TOPICS_BY_CONSUMER)
    assert request.ext_fields == {"group": GROUP}
    assert TOPIC in result.get_topic_list()


def test_query_topics_by_consumer_routes_by_retry_topic_and_dedupes():
    """Java `DefaultMQAdminExtImpl:1078`：路由查的是 `%RETRY%<group>`，逐 broker 扇出后
    合并进 `Set<String>`。路由查错 topic（业务 topic）在单 broker 集群上看不出来，
    多 broker 时才会静默漏掉整个 topic。
    """
    client = _FakeClient(
        [_ok(_topic_list_body([TOPIC, GROUP_RETRY])), _ok(_topic_list_body([TOPIC]))],
        route=_route(("broker-a", ADDR_A), ("broker-b", ADDR_B)))
    admin = _admin(client)

    result = admin.query_topics_by_consumer(GROUP)

    assert client.route_lookups == [GROUP_RETRY]
    assert [addr for addr, _ in client.requests] == [ADDR_A, ADDR_B]
    assert sorted(result.get_topic_list()) == sorted({TOPIC, GROUP_RETRY})


def test_query_topics_by_consumer_returns_empty_topic_list_when_group_has_no_offset():
    """broker 的 `whichTopicByConsumer` 读位点表：没提交过位点的组回空表而不是报错。"""
    client = _FakeClient([_ok(_topic_list_body([]))], route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)

    assert admin.query_topics_by_consumer(GROUP).get_topic_list() == []
