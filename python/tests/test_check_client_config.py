# -*- coding: utf-8 -*-
"""CHECK_CLIENT_CONFIG(46) 的离线单测（对应 Java MQClientInstance#checkClientInBroker）。

为什么要有这一步：SQL92 表达式写错时 broker **不会**报错 ——
``ExpressionMessageFilter#isMatched`` 在 ConsumeQueue 阶段拿不到编译好的过滤数据就直接
``return true``（静默放行全部消息），消费者启动照常成功。Java 因此在
``DefaultMQPushConsumerImpl.start:1014`` 主动发一笔 46 号请求，把「写错的表达式」
变成启动期错误。本端口此前只有 RequestCode 常量、没有调用方，所以这里锁死协议形状
与分支语义（真机行为见 ``verify_sql92_live.py``）。

对齐基准（Java 5.5.1，逐行读过）：
  * ``MQClientAPIImpl#checkClientInBroker:3256``：请求头是 **null**，body 是
    ``CheckClientRequestBody`` 的 JSON（clientId / group / subscriptionData）；
    响应码非 SUCCESS 时抛 ``MQClientException(响应码, remark)``。
  * ``MQClientInstance#checkClientInBroker:534``：只查非 TAG 订阅
    （``ExpressionType.isTagType``：null / "" / TAG 都算 TAG）；broker 地址来自
    ``findBrokerAddrByTopic``（随机一个 broker、优先 master），取不到就**跳过**；
    某个消费者「无订阅」时是 ``return`` 而不是 ``continue``（Java 源码如此）；
    网络类异常包装成固定文案的 MQClientException（"…server has not been upgraded…"）。
  * ``ClientConfig#mqClientApiTimeout`` 默认 **3000ms**，这笔请求用的就是它。
"""
from __future__ import annotations

import json
from typing import List, Optional

import pytest

from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.exception import MQClientException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.common.subscription_data import ExpressionType, SubscriptionData
from rocketmq.remoting.exception import RemotingConnectException
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

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
    """替身 remoting 层：记下每一笔请求，按脚本回放响应码（None 表示抛异常）。"""

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

    # MQClientInstance 只在 _invoke_sync 里用到 remoting_client，其余方法不碰
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
        """只看 46 号请求：路由缓存空时 ``find_broker_addr_by_topic`` 会顺手补拉一次路由
        （GET_ROUTEINFO_BY_TOPIC 也会打到这里），那是预期内的副作用，不该影响断言。
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
    """TAG（含 null/空串）一律不发 46：Java ``ExpressionType.isTagType`` 短路。"""
    inst, rec = _instance(
        _sub("TagTopic", "tagA || tagB", ExpressionType.TAG),
        _sub("NullTypeTopic", "*", None),
        _sub("EmptyTypeTopic", "*", ""),
    )
    inst.check_client_in_broker()
    assert rec.requests == []


def test_sql92_request_shape_matches_java():
    inst, rec = _instance(_sub())
    inst.check_client_in_broker()
    assert rec.codes == [RequestCode.CHECK_CLIENT_CONFIG]
    addr, req, timeout = rec.check_requests[0]
    assert addr == BROKER
    # 请求头 null ⇒ 线上没有 extFields；body 是 CheckClientRequestBody 的 JSON
    assert not req.ext_fields
    assert timeout == 3000                     # Java ClientConfig.mqClientApiTimeout
    body = rec.body_of()
    assert body["clientId"] == CLIENT_ID
    assert body["group"] == GROUP
    sd = body["subscriptionData"]
    assert sd["topic"] == TOPIC
    assert sd["subString"] == "a > 10"
    assert sd["expressionType"] == ExpressionType.SQL92
    # Java SubscriptionData 的序列化字段名（filterClassSource 是 @JSONField(serialize=false)）
    assert sorted(sd.keys()) == ["classFilterMode", "codeSet", "expressionType",
                                 "subString", "subVersion", "tagsSet", "topic"]
    assert "filterClassSource" not in sd


def test_broker_reject_code_becomes_client_exception():
    """SUBSCRIPTION_PARSE_FAILED(23) 原样带上响应码 —— 这是启动失败的判据。"""
    inst, rec = _instance(_sub(expression="a >"))
    rec.responses = [ResponseCode.SUBSCRIPTION_PARSE_FAILED]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    assert ei.value.response_code == ResponseCode.SUBSCRIPTION_PARSE_FAILED
    assert "remark" in str(ei.value)


def test_broker_without_property_filter_support_reports_system_error():
    """broker 未开 enablePropertyFilter 时回 SYSTEM_ERROR(1)（Java 同码）。"""
    inst, rec = _instance(_sub())
    rec.responses = [ResponseCode.SYSTEM_ERROR]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    assert ei.value.response_code == ResponseCode.SYSTEM_ERROR


def test_no_route_skips_the_subscription():
    """查不到路由 → 跳过（Java findBrokerAddrByTopic 返回 null 即 continue）。

    缓存被清掉后 ``find_broker_addr_by_topic`` 会先补拉一次路由（105），这里让它回空 body，
    所以最终仍然一笔 46 都不发。
    """
    inst, rec = _instance(_sub())
    inst.topic_route_table.clear()
    inst.check_client_in_broker()
    assert rec.check_requests == []


def test_transport_error_is_wrapped_with_java_message():
    """连不上 broker 时 Java 不吞异常，而是换成一段固定文案再抛。"""
    inst, rec = _instance(_sub())
    rec.responses = [RemotingConnectException(BROKER)]
    with pytest.raises(MQClientException) as ei:
        inst.check_client_in_broker()
    msg = str(ei.value)
    assert "SQL92" in msg
    assert "server has not been upgraded to support" in msg
    assert isinstance(ei.value.cause, RemotingConnectException)


def test_empty_subscription_returns_whole_check():
    """Java 的 ``return``（不是 ``continue``）：空订阅的消费者会把后面的检查一起终止。"""
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
    """consumerTable 只收推模式消费者；未登记的调用方用内层循环拿同样的语义。"""
    inst, rec = _instance(_sub())
    inst.check_subscriptions_in_broker(GROUP, [_sub()])
    assert rec.codes == [RequestCode.CHECK_CLIENT_CONFIG]
    assert rec.body_of()["group"] == GROUP
