# -*- coding: utf-8 -*-
"""生产者退出时发 ``UNREGISTER_CLIENT``(35) 的离线接线与上线形状。

Java 链路：``DefaultMQProducerImpl.shutdown():313`` → ``MQClientInstance.unregisterProducer:1198-1201``
→ 私有 ``unregisterClient(group, null):1158-1182`` —— 逐台 broker 同步发 35，
**没用到的那个槽位传 null**（字段压根不上线），异常只 log.warn。
真机侧的证据在 ``verify_producer_unregister_live.py``（抓帧 + 204 前后对照），
Rust 还有一套「同 clientId 共用一条连接」的判别式证明（``rust/examples/live_producer.rs`` P11）；
这里锁的是不会退回来的三件事：

* 注销排在**关客户端之前**（35 只能走还开着的长连接）；
* 注销失败不能把 ``shutdown()`` 打断；
* 空组名不上线（broker ``ClientManageProcessor:228/237`` 判的是 ``group != null``）。
"""
from __future__ import annotations

from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.remoting.protocol.codes import RequestCode


class _FakeClient:
    """记录 shutdown 路径对本客户端实例的调用顺序。"""

    def __init__(self, fail_unregister: bool = False):
        self.calls = []
        self.fail_unregister = fail_unregister

    def unregister_client_all_brokers(self, client_id, producer_group, consumer_group,
                                      timeout_millis=3000):
        self.calls.append(("unregister", client_id, producer_group, consumer_group,
                           timeout_millis))
        if self.fail_unregister:
            raise RuntimeError("broker gone away")

    def shutdown(self):
        self.calls.append(("shutdown",))


class _InvokeSpy:
    """只替掉 ``_invoke_sync`` / ``_check_response``，跑真实的 ``unregister_client``。"""

    def __init__(self):
        self.requests = []
        self.timeouts = []

    def _invoke_sync(self, addr, request, timeout_millis):
        self.requests.append((addr, request))
        self.timeouts.append(timeout_millis)
        return _OkResponse()

    def _check_response(self, response):
        pass


class _OkResponse:
    code = 0
    remark = None
    body = b""


def _producer(client: _FakeClient) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_unreg_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.client_id = "30.0.0.1@unreg-offline"
    p._mq_client = client
    p._started = True
    return p


def test_producer_shutdown_unregisters_before_closing_the_client():
    client = _FakeClient()
    p = _producer(client)
    p.shutdown()
    kinds = [c[0] for c in client.calls]
    assert kinds == ["unregister", "shutdown"], client.calls
    _, cid, group, consumer_group, _timeout = client.calls[0]
    assert cid == p.client_id
    assert group == "GID_unreg_test"
    # 生产者退出：消费者槽位留空（上线时会被整个丢掉）
    assert consumer_group == ""


def test_unregister_failure_does_not_break_shutdown():
    """Java 的 unregisterClient 把异常吞成 log.warn，shutdown 必须继续关客户端。"""
    client = _FakeClient(fail_unregister=True)
    p = _producer(client)
    p.shutdown()
    assert [c[0] for c in client.calls] == ["unregister", "shutdown"]
    assert p._started is False


def test_blank_group_is_not_put_on_the_wire():
    """空组名必须整个不上线：Java 传 null，broker 用 ``group != null`` 决定摘哪一侧。"""
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "GID_x", "", 3000)
    addr, request = spy.requests[0]
    assert addr == "127.0.0.1:10911"
    assert request.code == RequestCode.UNREGISTER_CLIENT
    ext = request.custom_header.to_ext_fields()
    assert ext == {"clientID": "cid-1", "producerGroup": "GID_x"}, ext


def test_consumer_side_blank_producer_group_is_dropped_too():
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "", "GID_c", 3000)
    ext = spy.requests[0][1].custom_header.to_ext_fields()
    assert ext == {"clientID": "cid-1", "consumerGroup": "GID_c"}, ext


def test_both_sides_present_when_given():
    """两个槽位都有值时按 Java 的字段顺序上线（clientID, producerGroup, consumerGroup）。"""
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "GID_p", "GID_c", 3000)
    ext = spy.requests[0][1].custom_header.to_ext_fields()
    assert list(ext.items()) == [("clientID", "cid-1"), ("producerGroup", "GID_p"),
                                 ("consumerGroup", "GID_c")]


def test_whitespace_group_is_dropped_too():
    """纯空白与空串同处理：合法组名不可能全是空白（Validators 那一关过不去）。"""
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "   ", "GID_c", 3000)
    ext = spy.requests[0][1].custom_header.to_ext_fields()
    assert ext == {"clientID": "cid-1", "consumerGroup": "GID_c"}, ext


def test_unregister_fans_out_to_every_broker_including_slaves():
    """Java ``unregisterClient``:1158-1182 遍历 brokerAddrTable 的**每个 brokerId**：
    Producer/ConsumerManager 是每台 broker 各自一份状态，漏掉从节点就等于那台的注册要等
    通道扫描（默认 ~120s）才回收。心跳那侧仍只打 master —— 两个 helper 的分工一起锁住。
    """
    from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

    inst = MQClientInstance("unreg-fanout", ["127.0.0.1:9876"])
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a",
                                     {0: "127.0.0.1:10911", 1: "127.0.0.1:10912"})]
    inst.topic_route_table["FanoutTopic"] = route

    seen = []
    inst.unregister_client = lambda addr, cid, pg, cg, timeout=3000: seen.append(  # noqa: E731
        (addr, cid, pg, cg))
    inst.unregister_client_all_brokers("cid-1", "GID_p", "")
    assert seen == [("127.0.0.1:10911", "cid-1", "GID_p", ""),
                    ("127.0.0.1:10912", "cid-1", "GID_p", "")], seen
    # 心跳/探活那侧依旧「一 brokerName 一台、master 优先」
    assert inst.get_route_of_all_brokers() == ["127.0.0.1:10911"]
    assert inst.get_all_broker_addrs() == ["127.0.0.1:10911", "127.0.0.1:10912"]


def test_unregister_failure_on_one_broker_does_not_stop_the_fanout():
    """单台失败只 log.debug：Java 的 catch 是 log.warn，剩下那台照样要注销到。"""
    from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

    inst = MQClientInstance("unreg-fanout-fail", ["127.0.0.1:9876"])
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a",
                                     {0: "127.0.0.1:10911", 1: "127.0.0.1:10912"})]
    inst.topic_route_table["FanoutTopic"] = route

    seen = []

    def boom(addr, cid, pg, cg, timeout=3000):
        seen.append(addr)
        if addr.endswith("10911"):
            raise RuntimeError("broker gone away")

    inst.unregister_client = boom
    inst.unregister_client_all_brokers("cid-1", "GID_p", "")
    assert seen == ["127.0.0.1:10911", "127.0.0.1:10912"], seen


def test_unregister_budget_is_javas_mq_client_api_timeout():
    """默认超时必须是 Java 的 ``getMqClientApiTimeout()`` = 3000ms（``ClientConfig.java:81``；
    ``MQClientInstance#unregisterClient``:1170 传的就是它）。

    ``shutdown()`` 那条路径不显式传参，所以这个默认值就是线上真正生效的预算。写歪成 5000
    不会让任何用例变红 —— 只是每台 broker 多等 2s、整体退出慢一档，因此单独立一条盯着它。
    """
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "GID_x", "")
    assert spy.timeouts == [3000], spy.timeouts

    from rocketmq.remoting.protocol.route import BrokerData, TopicRouteData

    inst = MQClientInstance("unreg-timeout", ["127.0.0.1:9876"])
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a",
                                     {0: "127.0.0.1:10911", 1: "127.0.0.1:10912"})]
    inst.topic_route_table["TimeoutTopic"] = route
    seen_timeouts = []
    inst.unregister_client = lambda addr, cid, pg, cg, timeout=5000: \
        seen_timeouts.append(timeout)  # noqa: E731
    inst.unregister_client_all_brokers("cid-1", "GID_p", "")
    # 扇出的每一发都拿到同一份预算，且是默认值（调用方没传）
    assert seen_timeouts == [3000, 3000], seen_timeouts
