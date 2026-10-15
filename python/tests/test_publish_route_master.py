# -*- coding: utf-8 -*-
from __future__ import annotations

import pytest

from client.exception import MQClientException
from client.mq_client import MQClientInstance
from common.message import MessageQueue
from remoting.exception import (RemotingConnectException,
                                         RemotingTimeoutException)
from remoting.protocol.codes import ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, QueueData, TopicRouteData

MASTER = 0
SLAVE = 1


def _route(queue_datas, broker_datas) -> TopicRouteData:
    route = TopicRouteData()
    route.queue_datas = list(queue_datas)
    route.broker_datas = list(broker_datas)
    return route


def test_publish_queues_skip_a_broker_whose_master_is_gone():
    route = _route(
        [QueueData("broker-a", 4, 4, 6, 0)],          # master offline, write queue count still 6
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert route.get_all_message_queue("T") == []


def test_publish_queues_come_back_once_the_master_registers():
    """Negative control: what is skipped is "no master", not the name broker-a."""
    bd = BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})
    route = _route([QueueData("broker-a", 4, 4, 6, 0)], [bd])
    assert route.get_all_message_queue("T") == []
    bd.broker_addrs[MASTER] = "127.0.0.1:10911"       # master registers again
    assert route.get_all_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(4)]


def test_publish_queues_keep_the_healthy_broker_only():
    route = _route(
        [QueueData("broker-a", 2, 2, 6, 0), QueueData("broker-b", 3, 3, 6, 0)],
        [
            BrokerData("c", "broker-a", {SLAVE: "a:10931"}),
            BrokerData("c", "broker-b", {MASTER: "b:10911", SLAVE: "b:10931"}),
        ],
    )
    assert route.get_all_message_queue("T") == [
        MessageQueue("T", "broker-b", i) for i in range(3)]


def test_subscribe_info_keeps_the_masterless_broker():
    pass
    route = _route(
        [QueueData("broker-a", 2, 2, 6, 0)],
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert route.get_all_subscribe_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(2)]


def test_subscribe_info_uses_read_perm_and_read_queue_nums():
    pass
    route = _route(
        [QueueData("broker-a", 3, 1, 4, 0)],
        [BrokerData("c", "broker-a", {MASTER: "a:10911"})],
    )
    assert route.get_all_message_queue("T") == []
    assert route.get_all_subscribe_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(3)]


def test_instance_subscribe_info_is_served_from_the_route_table():
    """The consumer's entry get_topic_subscribe_info: if the route is already in the table, queues are returned directly (no namesrv query)."""
    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    inst.topic_route_table["T"] = _route(
        [QueueData("broker-a", 2, 2, 6, 0)],
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert inst.get_topic_subscribe_info("T") == [
        MessageQueue("T", "broker-a", i) for i in range(2)]


# ---------------------------------------------- Address side: findBrokerAddressInPublish
def _route_of(addrs_by_broker) -> TopicRouteData:
    return _route(
        [QueueData(name, 2, 2, 6, 0) for name in addrs_by_broker],
        [BrokerData("c", name, addrs) for name, addrs in addrs_by_broker.items()],
    )


def _serving(route_holder):
    """An offline instance: ``_invoke_sync`` always replies with the ``route_holder[0]`` route (None = namesrv says there is none).

    Route refresh goes through the **real** ``update_topic_route_info_from_name_server`` -- the write point of the publish address flat table
    is inside it; bypassing it to stuff values directly into ``broker_addr_table`` would mean this path is not tested.
    """
    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    calls = []

    def fake_invoke(addr, request, timeout_millis=None):
        calls.append(addr)
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.ext_fields = {"offset": "7"}
        route = route_holder[0]
        response.body = route.encode() if route is not None else None
        return response

    inst._invoke_sync = fake_invoke     # type: ignore[method-assign]
    return inst, calls


def test_publish_lookup_takes_only_the_master_while_the_admin_lookup_falls_back():
    """``findBrokerAddressInPublish`` only accepts brokerId=0; on the same route the admin lookup must fall back to the slave."""
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, _ = _serving(holder)
    assert inst.update_topic_route_info_from_name_server("T") is True
    assert inst.find_broker_address_in_publish("broker-a") is None
    # negative control: the concession lookup (used by heartbeat/pull/offset queries) must be able to get the slave address on the **same route**
    assert inst.broker_addr_of("broker-a") == "127.0.0.1:10931"

    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    assert inst.find_broker_address_in_publish("broker-a") == "127.0.0.1:10911"


def test_publish_addr_for_refreshes_the_route_then_rechecks():
    pass
    holder = [None]      # this topic has not been seen yet
    inst, calls = _serving(holder)
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911"}})
    assert inst.publish_addr_for("broker-a", "T") == "127.0.0.1:10911"
    assert calls == ["127.0.0.1:9876"]


def test_publish_addr_for_reports_not_exist_when_the_master_is_gone():
    """Master offline (only brokerId=1 left in the route): the client reports "broker does not exist", sending no requests at all."""
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _serving(holder)
    with pytest.raises(MQClientException) as exc:
        inst.publish_addr_for("broker-a", "T")
    assert str(exc.value) == "The broker[broker-a] not exist"
    assert exc.value.response_code is None
    assert calls == ["127.0.0.1:9876"]
    # negative control: what is skipped is "no master", not the name broker-a -- once the master registers it resolves immediately
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    assert inst.publish_addr_for("broker-a", "T") == "127.0.0.1:10911"


def test_publish_addr_for_reports_not_exist_for_an_unknown_broker():
    """The route does not have this brokerName at all (typo/offline): report not exist the same way, without falling back to another broker."""
    holder = [_route_of({"broker-b": {MASTER: "127.0.0.1:10912"}})]
    inst, _ = _serving(holder)
    with pytest.raises(MQClientException) as exc:
        inst.publish_addr_for("broker-a", "T")
    assert str(exc.value) == "The broker[broker-a] not exist"


def test_admin_offset_queries_are_master_only_too():
    """The four offset queries in ``MQAdminImpl:195-264`` share the same criterion: master only, and if the master is gone, report not exist.

    The four queries resolve the address named at the master endpoint through the same entry: ``earliest_msg_store_time`` has no client-layer method,
    so on this side it lands on ``_publish_addr_in_admin`` (admin.py:949 / producer.py:2045 / consumer.py:3689
    all three facades only fill in the address, not resolve individually), so it is pinned down here together with the others.
    """
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _serving(holder)
    mq = MessageQueue("T", "broker-a", 0)

    # master gone: all four queries report client-side errors, sending no broker requests at all (the old behavior was to fall back to the slave and finish the query there)
    queries = {
        "maxOffset": lambda: inst.get_max_offset(mq),
        "minOffset": lambda: inst.get_min_offset(mq),
        "searchOffset": lambda: inst.search_offset_by_timestamp(mq, 1700000000000),
        "earliestMsgStoreTime": lambda: inst._publish_addr_in_admin(mq),
    }
    for name, call in queries.items():
        with pytest.raises(MQClientException) as exc:
            call()
        assert str(exc.value) == "The broker[broker-a] not exist", name
        assert exc.value.response_code is None, name

    # negative control: after the master comes back all four resolve, and requests land on the **master** address (not the slave in the route)
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    assert inst.get_max_offset(mq) == 7
    assert inst.get_min_offset(mq) == 7
    assert inst.search_offset_by_timestamp(mq, 1700000000000) == 7
    assert inst._publish_addr_in_admin(mq) == "127.0.0.1:10911"
    # the slave address was never hit once (three queries, one master each; the transfer resolution does not hit the broker)
    assert calls[-3:] == ["127.0.0.1:10911"] * 3
    assert "127.0.0.1:10931" not in calls


# -------------------------------------- Subscription side: findBrokerAddressInSubscribe(MASTER_ID, true)
def _invoking(route_holder):
    """An offline instance: namesrv replies with ``route_holder[0]``; broker requests are answered by code and each ``(addr, code)`` is recorded."""
    from remoting.protocol.body import LockBatchRequestBody, LockBatchResponseBody
    from remoting.protocol.codes import RequestCode

    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    calls = []

    def fake_invoke(addr, request, timeout_millis=None):
        calls.append((addr, request.code))
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.ext_fields = {}
        if request.code == RequestCode.GET_ROUTEINFO_BY_TOPIC:
            route = route_holder[0]
            response.body = route.encode() if route is not None else None
        elif request.code == RequestCode.LOCK_BATCH_MQ:
            # echo the request body's mqSet back as lockOKMQSet verbatim: a non-empty lock set proves "this shot really landed"
            rb = LockBatchResponseBody()
            rb.lock_ok_mq_set = list(LockBatchRequestBody.decode(request.body).mq_set)
            response.body = rb.encode()
        elif request.code == RequestCode.QUERY_CONSUMER_OFFSET:
            response.ext_fields = {"offset": "424242" if addr.endswith("10931") else "111"}
        elif request.code == RequestCode.POP_MESSAGE:
            response.code = ResponseCode.POLLING_TIMEOUT     # a long poll coming back empty-handed is the norm
        return response

    inst._invoke_sync = fake_invoke     # type: ignore[method-assign]
    return inst, calls


def test_orderly_locks_skip_the_broker_when_the_master_is_gone():
    pass
    from remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    mqs = [MessageQueue("T", "broker-a", 0), MessageQueue("T", "broker-a", 1)]

    assert inst.lock_batch_mq("GID", "cid", mqs) == []
    inst.unlock_batch_mq("GID", "cid", mqs)
    assert calls == []

    # negative control: after the master comes back lock/unlock both land on the master address, and the returned lock set is exactly the two requested queues
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    locked = inst.lock_batch_mq("GID", "cid", mqs)
    inst.unlock_batch_mq("GID", "cid", mqs)
    assert [(m.topic, m.broker_name, m.queue_id) for m in locked] == [
        ("T", "broker-a", 0), ("T", "broker-a", 1)]
    assert calls == [("127.0.0.1:10911", RequestCode.LOCK_BATCH_MQ),
                     ("127.0.0.1:10911", RequestCode.UNLOCK_BATCH_MQ)]


def test_pop_message_is_master_only_and_reports_not_exist():
    pass
    from client.consumer_result import PopStatus
    from remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()

    with pytest.raises(MQClientException) as exc:
        inst.pop_message("GID", "T", broker_name="broker-a")
    assert str(exc.value) == "The broker[broker-a] not exist"
    assert exc.value.response_code is None
    # the route must be refreshed once before reporting the error (popAsync's findBrokerAddressInSubscribe + recheck)
    assert calls == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC)]

    # negative control: after the master registers (this shot triggers a route refresh along the way) requests land on the master address, with none on the slave
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    calls.clear()
    result = inst.pop_message("GID", "T", broker_name="broker-a")
    assert result.status == PopStatus.POLLING_NOT_FOUND
    assert (("127.0.0.1:10911", RequestCode.POP_MESSAGE) in calls)
    assert all(addr != "127.0.0.1:10931" for addr, _ in calls)


def test_consumer_offset_falls_back_to_the_slave_after_a_refresh():
    pass
    from remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    mq = MessageQueue("T", "broker-a", 0)

    assert inst.query_consumer_offset("GID", mq) == 424242      # the slave's copy
    assert calls == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC),
                     ("127.0.0.1:10931", RequestCode.QUERY_CONSUMER_OFFSET)]

    # negative control: after the master comes back only the master is hit (the slave count stops growing)
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    assert inst.query_consumer_offset("GID", mq) == 111
    assert calls == [("127.0.0.1:10911", RequestCode.QUERY_CONSUMER_OFFSET)]

    # the route does not have broker-a at all (not even a slave): still not found after one route refresh => client reports not exist
    holder2 = [_route_of({"broker-b": {MASTER: "127.0.0.1:10912"}})]
    inst2, calls2 = _invoking(holder2)
    with pytest.raises(MQClientException) as exc:
        inst2.query_consumer_offset("GID", mq)
    assert str(exc.value) == "The broker[broker-a] not exist"
    assert calls2 == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC)]


# ---------------------------------------------- NameServer failover (the semantics of _fetch)
def _ns_instance(addrs):
    """An offline instance with two name servers; returns (inst, scripted response list).

    The script list is consumed in address order: each item is a ``RemotingCommand`` or an exception instance.
    This is the mock version of "kill ns[0] under multiple NameServers" -- the mock layer builds no TCP,
    connection failures are expressed directly as ``RemotingConnectException``, consistent with real behavior.
    """
    inst = MQClientInstance("route-test@unit", list(addrs))
    script = []

    def fake_invoke(addr, request, timeout_millis=None):
        outcome = script.pop(0) if script else None
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome if outcome is not None else RemotingCommand()

    inst._invoke_sync = fake_invoke     # type: ignore[method-assign]
    return inst, script


def _route_ok(topic="T"):
    response = RemotingCommand()
    response.code = ResponseCode.SUCCESS
    response.body = _route_of({"broker-a": {MASTER: "127.0.0.1:10911"}}).encode()
    return response


def _topic_not_exist():
    response = RemotingCommand()
    response.code = ResponseCode.TOPIC_NOT_EXIST
    response.remark = "topic[T] not exist"
    return response


def test_route_fetch_fails_over_to_the_surviving_name_server():
    """ns[0] unreachable + ns[1] answers normally => the route must be obtained (the producer-side failover path)."""
    inst, script = _ns_instance(["127.0.0.1:19876", "127.0.0.1:9876"])
    script.append(RemotingConnectException("127.0.0.1:19876"))
    script.append(_route_ok())
    assert inst.update_topic_route_info_from_name_server("T") is True
    assert inst.find_broker_address_in_publish("broker-a") == "127.0.0.1:10911"


def test_route_fetch_never_reraises_a_stale_connect_error():
    """The defect this file guards: ns[0] unreachable + ns[1] gives an **authoritative reply** (TOPIC_NOT_EXIST).

    %RETRY%group genuinely has no route for a new group -- ns[1]'s reply is final and must not be
    overridden by that one connection exception from ns[0]. Real scenario: kill ns[0] under multiple NameServers, and the consumer's start()
    throws RemotingConnectException directly in _refresh_routes() (the producer always gets SUCCESS,
    so only the consumer side blows up). After the fix it must be "False + no throw", letting _refresh_routes' MQClientException fallback record it as "no route for now".
    """
    inst, script = _ns_instance(["127.0.0.1:19876", "127.0.0.1:9876"])
    script.append(RemotingConnectException("127.0.0.1:19876"))
    script.append(_topic_not_exist())
    assert inst.update_topic_route_info_from_name_server("%RETRY%GID_x") is False


def test_route_fetch_raises_only_when_every_name_server_is_unreachable():
    pass
    inst, script = _ns_instance(["127.0.0.1:19876", "127.0.0.1:19877"])
    script.append(RemotingConnectException("127.0.0.1:19876"))
    script.append(RemotingConnectException("127.0.0.1:19877"))
    with pytest.raises(RemotingConnectException):
        inst.update_topic_route_info_from_name_server("T")


def test_route_fetch_timeout_also_moves_to_the_next_name_server():
    """A timeout is on par with a connection failure: both are transport-level failures, so move to the next one the same way."""
    inst, script = _ns_instance(["127.0.0.1:19876", "127.0.0.1:9876"])
    script.append(RemotingTimeoutException("127.0.0.1:19876", 5000))
    script.append(_route_ok())
    assert inst.update_topic_route_info_from_name_server("T") is True
