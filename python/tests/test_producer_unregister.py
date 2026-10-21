# -*- coding: utf-8 -*-
from __future__ import annotations

from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from remoting.protocol.codes import RequestCode


class _FakeClient:
    """Record the order of calls made to this client instance on the shutdown path."""

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
    """Replace only ``_invoke_sync`` / ``_check_response``, running the real ``unregister_client``."""

    def __init__(self):
        self.requests = []
        self.timeouts = []
        self.mq_client_api_timeout = 3000

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
    # producer shutdown: the consumer slot stays empty (it gets dropped entirely on registration)
    assert consumer_group == ""


def test_unregister_failure_does_not_break_shutdown():
    pass
    client = _FakeClient(fail_unregister=True)
    p = _producer(client)
    p.shutdown()
    assert [c[0] for c in client.calls] == ["unregister", "shutdown"]
    assert p._started is False


def test_blank_group_is_not_put_on_the_wire():
    pass
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
    pass
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "GID_p", "GID_c", 3000)
    ext = spy.requests[0][1].custom_header.to_ext_fields()
    assert list(ext.items()) == [("clientID", "cid-1"), ("producerGroup", "GID_p"),
                                 ("consumerGroup", "GID_c")]


def test_whitespace_group_is_dropped_too():
    """Whitespace-only strings are treated the same as empty ones: a legal group name cannot be all whitespace (it would not pass Validators)."""
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "   ", "GID_c", 3000)
    ext = spy.requests[0][1].custom_header.to_ext_fields()
    assert ext == {"clientID": "cid-1", "consumerGroup": "GID_c"}, ext


def test_unregister_fans_out_to_every_broker_including_slaves():
    pass
    from remoting.protocol.route import BrokerData, TopicRouteData

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
    # the heartbeat/liveness side still keeps "one per brokerName, master first"
    assert inst.get_route_of_all_brokers() == ["127.0.0.1:10911"]
    assert inst.get_all_broker_addrs() == ["127.0.0.1:10911", "127.0.0.1:10912"]


def test_unregister_failure_on_one_broker_does_not_stop_the_fanout():
    pass
    from remoting.protocol.route import BrokerData, TopicRouteData

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


def test_consumer_heartbeat_fans_out_to_slaves_while_producer_stays_master_only():
    pass
    from client.consumer import DefaultMQPushConsumer
    from client.producer import DefaultMQProducer
    from remoting.protocol.route import BrokerData, TopicRouteData

    master, slave = "127.0.0.1:10911", "127.0.0.1:10912"

    def instance(tag):
        inst = MQClientInstance(tag, ["127.0.0.1:9876"])
        route = TopicRouteData()
        route.broker_datas = [BrokerData("DefaultCluster", "broker-hb",
                                         {0: master, 1: slave})]
        inst.topic_route_table["HbTopic"] = route
        return inst

    cinst = instance("hb-consumer")
    sent = []
    cinst.send_heartbeat = lambda addr, hb, timeout=5000: sent.append((addr, hb))  # noqa: E731
    c = DefaultMQPushConsumer("GID_hb_fanout")
    c.client_id = "30.0.0.1@hb-offline"
    c._mq_client = cinst
    c._started = True
    assert c._send_heartbeat_to_all_broker() == 2
    assert [addr for addr, _ in sent] == [master, slave], sent
    # the copy a slave receives must **carry the consumer group**, otherwise its ConsumerManager cannot build the group info
    for _addr, hb in sent:
        assert [cd.group_name for cd in hb.consumer_data_set] == ["GID_hb_fanout"]
        assert hb.client_id == "30.0.0.1@hb-offline"

    # negative control for the producer: same route, same address table, only the master is allowed
    pinst = instance("hb-producer")
    psent = []
    pinst.send_heartbeat = lambda addr, hb, timeout=5000: psent.append(addr)  # noqa: E731
    p = DefaultMQProducer("GID_hb_producer")
    p.client_id = "30.0.0.1@hb-offline"
    p._mq_client = pinst
    p._started = True
    assert p._send_heartbeat_to_all_broker() == 1
    assert psent == [master], psent


def test_unregister_budget_is_the_client_api_timeout():
    pass
    spy = _InvokeSpy()
    MQClientInstance.unregister_client(spy, "127.0.0.1:10911", "cid-1", "GID_x", "")
    assert spy.timeouts == [3000], spy.timeouts

    from remoting.protocol.route import BrokerData, TopicRouteData

    inst = MQClientInstance("unreg-timeout", ["127.0.0.1:9876"])
    route = TopicRouteData()
    route.broker_datas = [BrokerData("DefaultCluster", "broker-a",
                                     {0: "127.0.0.1:10911", 1: "127.0.0.1:10912"})]
    inst.topic_route_table["TimeoutTopic"] = route
    seen_timeouts = []
    inst.unregister_client = lambda addr, cid, pg, cg, timeout=5000: \
        seen_timeouts.append(timeout)  # noqa: E731
    inst.unregister_client_all_brokers("cid-1", "GID_p", "")
    # every fan-out shot gets the same budget, and it is the default (the caller passed nothing)
    assert seen_timeouts == [3000, 3000], seen_timeouts
