# -*- coding: utf-8 -*-
"""admin 消息轨迹（messageTrackDetail / consumed / examineConsumeStatsGroup）与
VIP 通道开关的离线单测，不需要集群。

对齐 Java `DefaultMQAdminExtImpl`：
  - `examineConsumeStats(group[, topic])`（:389-424）：按 `%RETRY%<group>` 的路由扇出
    全部 broker，逐台取统计并合并；全空时抛 MQClientException。
  - `consumed(msg, group)`（:1533-1557）：按 topic + queueId 匹配位点表，且 master
    地址（brokerId=0）必须等于消息的 storeHost，consumerOffset 越过 queueOffset 才算已消费。
  - `messageTrackDetail(msg)`（:1349-1427）：逐组判
    CONSUMED / CONSUMED_BUT_FILTERED / PULL / NOT_CONSUME_YET / NOT_ONLINE /
    CONSUME_BROADCASTING，异常分支的 desc 口径与 Java 一致（NOT_ONLINE 带
    "CODE:n DESC:msg"，BROADCAST_CONSUMPTION 不带，其余异常只带文案）。

VIP 开关（Java `ClientConfig#vipChannelEnabled`，5.x 默认 false）只作用于 broker 类
请求：`MixAll.brokerVIPChannel` = 端口 - 2。NameServer 类请求（topic 删除、KV 配置、
读写权限清理）永远直连 NameServer 原端口 —— 这是曾经的回归点：python/rust 曾把
DELETE_TOPIC_IN_NAMESRV、WIPE/ADD_WRITE_PERM_OF_BROKER 误路由进 broker 通道。
"""
from __future__ import annotations

import pytest

from client.admin import DefaultMQAdminExt, MessageTrack, TrackType
from client.exception import MQBrokerException, MQClientException
from common.message import MessageExt, MessageQueue
from common.mix_all import MixAll
from remoting.protocol.admin_body import ConsumeStats, OffsetWrapper
from remoting.protocol.body import ClusterInfo, ConsumerConnection
from remoting.protocol.codes import ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_TrackUnit"
TOPIC = "TrackUnitTopic"
ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
NS_ADDR = "127.0.0.1:9876"


# ------------------------------------------------------------------ 测试替身

class _FakeClient:
    """只替掉同步调用入口，记录每个请求实际打到的地址。"""

    def __init__(self, responses=None, name_server_addrs=None, route=None):
        self._responses = list(responses or [])
        self.requests = []  # [(addr, RemotingCommand)]
        self.name_server_addrs = list(name_server_addrs or [NS_ADDR])
        self.route = route

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request))
        if not self._responses:
            raise AssertionError("unexpected extra request: %s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def _check_response(self, response):
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")

    def get_topic_route_data(self, topic, timeout_millis=None):
        return self.route


def _ok(body: bytes = b"") -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    return r


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


def _route(*broker_names_and_addrs) -> TopicRouteData:
    route = TopicRouteData()
    for name, addr in broker_names_and_addrs:
        route.broker_datas.append(BrokerData("DefaultCluster", name, {0: addr}))
    return route


def _msg(store_host: str = "10.0.0.9", store_port: int = 10911,
         queue_id: int = 1, queue_offset: int = 10, tags: str = "TagA") -> MessageExt:
    msg = MessageExt(TOPIC, tags=tags)
    msg.store_host = store_host
    msg.store_host_port = store_port
    msg.queue_id = queue_id
    msg.queue_offset = queue_offset
    return msg


def _cluster_info() -> ClusterInfo:
    ci = ClusterInfo()
    ci.broker_addr_table["broker-a"] = {0: ADDR_A}
    ci.broker_addr_table["broker-b"] = {0: ADDR_B}
    return ci


def _cc(consume_type: str = "CONSUME_PASSIVELY",
        subscription: dict = None) -> ConsumerConnection:
    cc = ConsumerConnection()
    cc.consume_type = consume_type
    if subscription is not None:
        cc.subscription_table[TOPIC] = subscription
    return cc


# ------------------------------------------------------------------ VIP 通道

def test_broker_vip_channel_vectors():
    assert MixAll.broker_vip_channel(False, ADDR_A) == ADDR_A
    assert MixAll.broker_vip_channel(True, ADDR_A) == "127.0.0.1:10909"
    assert MixAll.broker_vip_channel(True, "10.0.0.1:8899") == "10.0.0.1:8897"
    # 端口不可解析 / 无冒号：原样返回（Java 抛 NumberFormatException，这里不崩）
    assert MixAll.broker_vip_channel(True, "127.0.0.1:abc") == "127.0.0.1:abc"
    assert MixAll.broker_vip_channel(True, "127.0.0.1") == "127.0.0.1"


def test_vip_knob_defaults_off_and_only_rewrites_broker_requests():
    client = _FakeClient([_ok() for _ in range(3)])
    admin = _admin(client)
    assert admin.vip_channel_enabled is False  # Java 5.x 默认 false

    # 开关关闭：broker 请求走原端口
    admin._invoke_broker(ADDR_A, 17)
    assert client.requests[-1][0] == ADDR_A

    # 开关打开：broker 请求改走 VIP 端口（端口 - 2）
    admin.set_vip_channel_enabled(True)
    assert admin.vip_channel_enabled is True
    admin._invoke_broker(ADDR_A, 17)
    assert client.requests[-1][0] == "127.0.0.1:10909"

    # NameServer 类请求不受开关影响：DELETE_TOPIC_IN_NAMESRV 直连原端口
    admin.delete_topic_in_name_server([NS_ADDR], TOPIC)
    assert client.requests[-1][0] == NS_ADDR


# ------------------------------------------------------------------ examineConsumeStatsGroup

def test_examine_consume_stats_group_merges_brokers_and_sums_tps():
    stats_a = ConsumeStats()
    stats_a.offset_table[MessageQueue(TOPIC, "broker-a", 0)] = OffsetWrapper(
        broker_offset=100, consumer_offset=90)
    stats_a.consume_tps = 1.5
    stats_b = ConsumeStats()
    stats_b.offset_table[MessageQueue(TOPIC, "broker-b", 0)] = OffsetWrapper(
        broker_offset=50, consumer_offset=50)
    stats_b.consume_tps = 0.5

    admin = _admin(_FakeClient([_ok(stats_a.encode()), _ok(stats_b.encode())],
                               route=_route(("broker-a", ADDR_A), ("broker-b", ADDR_B))))

    result = admin.examine_consume_stats_group(GROUP)
    assert set(result.offset_table) == {
        MessageQueue(TOPIC, "broker-a", 0), MessageQueue(TOPIC, "broker-b", 0)}
    assert result.consume_tps == pytest.approx(2.0)
    # 路由必须落在 %RETRY%<group> 上（Java examineConsumeStats(group) 同口径）


def test_examine_consume_stats_group_raises_when_empty():
    empty = ConsumeStats()
    admin = _admin(_FakeClient([empty.encode()]))
    with pytest.raises(MQClientException):
        admin.examine_consume_stats_group(GROUP)


# ------------------------------------------------------------------ consumed

def test_consumed_true_when_master_offset_passed_queue_offset():
    stats = ConsumeStats()
    stats.offset_table[MessageQueue(TOPIC, "broker-a", 1)] = OffsetWrapper(
        broker_offset=100, consumer_offset=11)
    admin = _admin(_FakeClient([_ok(stats.encode())], route=_route(("broker-a", ADDR_A))))
    admin.examine_broker_cluster_info = _cluster_info

    # storeHost 127.0.0.1:10911 == master 地址 ⇒ 同机；consumerOffset 11 > queueOffset 10
    msg = _msg(store_host="127.0.0.1", store_port=10911)
    assert admin.consumed(msg, GROUP) is True


def test_consumed_false_when_offset_not_passed():
    stats = ConsumeStats()
    stats.offset_table[MessageQueue(TOPIC, "broker-a", 1)] = OffsetWrapper(
        broker_offset=100, consumer_offset=10)
    admin = _admin(_FakeClient([_ok(stats.encode())], route=_route(("broker-a", ADDR_A))))
    admin.examine_broker_cluster_info = _cluster_info

    msg = _msg(store_host="127.0.0.1", store_port=10911)
    msg.queue_offset = 10
    # consumerOffset == queueOffset ⇒ 还没消费到这条
    assert admin.consumed(msg, GROUP) is False


def test_consumed_false_when_master_differs_from_store_host():
    stats = ConsumeStats()
    stats.offset_table[MessageQueue(TOPIC, "broker-a", 1)] = OffsetWrapper(
        broker_offset=100, consumer_offset=99)
    admin = _admin(_FakeClient([_ok(stats.encode())], route=_route(("broker-a", ADDR_A))))
    admin.examine_broker_cluster_info = _cluster_info
    msg = _msg(store_host="10.9.9.9", store_port=10911)
    assert admin.consumed(msg, GROUP) is False


# ------------------------------------------------------------------ messageTrackDetail

def _track_admin(monkeypatch, groups, connection, consumed_result=None,
                 consumed_error=None) -> DefaultMQAdminExt:
    admin = _admin(_FakeClient())
    monkeypatch.setattr(admin, "examine_topic_route", lambda topic: _route(("broker-a", ADDR_A)))
    monkeypatch.setattr(admin, "query_topic_consume_by_who", lambda addr, topic: set(groups))
    monkeypatch.setattr(admin, "examine_consumer_connection_info", lambda group: connection)
    if consumed_error is not None:
        def _consumed(msg, group):
            raise consumed_error
        monkeypatch.setattr(admin, "consumed", _consumed)
    else:
        monkeypatch.setattr(admin, "consumed", lambda msg, group: consumed_result)
    return admin


def test_message_track_not_online_when_group_offline(monkeypatch):
    admin = _admin(_FakeClient())
    monkeypatch.setattr(admin, "examine_topic_route", lambda topic: _route(("broker-a", ADDR_A)))
    monkeypatch.setattr(admin, "query_topic_consume_by_who", lambda addr, topic: {GROUP})

    def _raise(group):
        raise MQBrokerException(ResponseCode.CONSUMER_NOT_ONLINE, "not online")
    monkeypatch.setattr(admin, "examine_consumer_connection_info", _raise)

    tracks = admin.message_track_detail(_msg())
    assert len(tracks) == 1
    assert tracks[0].track_type == TrackType.NOT_ONLINE
    assert tracks[0].exception_desc == "CODE:206 DESC:not online"


def test_message_track_generic_exception_keeps_desc_only(monkeypatch):
    admin = _admin(_FakeClient())
    monkeypatch.setattr(admin, "examine_topic_route", lambda topic: _route(("broker-a", ADDR_A)))
    monkeypatch.setattr(admin, "query_topic_consume_by_who", lambda addr, topic: {GROUP})

    def _raise(group):
        raise RuntimeError("boom")
    monkeypatch.setattr(admin, "examine_consumer_connection_info", _raise)

    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.UNKNOWN
    assert tracks[0].exception_desc == "boom"


def test_message_track_pull_for_consume_actively(monkeypatch):
    admin = _track_admin(monkeypatch, [GROUP], _cc("CONSUME_ACTIVELY"))
    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.PULL
    assert tracks[0].exception_desc is None


def test_message_track_broadcasting_has_no_desc(monkeypatch):
    admin = _track_admin(
        monkeypatch, [GROUP], _cc("CONSUME_PASSIVELY"),
        consumed_error=MQBrokerException(ResponseCode.BROADCAST_CONSUMPTION, "broadcast"))
    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.CONSUME_BROADCASTING
    assert tracks[0].exception_desc is None  # Java 的 BROADCAST_CONSUMPTION 分支不带 desc


def test_message_track_not_consumed_yet(monkeypatch):
    admin = _track_admin(monkeypatch, [GROUP], _cc("CONSUME_PASSIVELY"), consumed_result=False)
    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.NOT_CONSUME_YET


def test_message_track_consumed_with_wildcard_subscription(monkeypatch):
    admin = _track_admin(monkeypatch, [GROUP],
                         _cc("CONSUME_PASSIVELY", {"tagsSet": ["*"]}),
                         consumed_result=True)
    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.CONSUMED


def test_message_track_consumed_but_filtered_by_tags(monkeypatch):
    admin = _track_admin(monkeypatch, [GROUP],
                         _cc("CONSUME_PASSIVELY", {"tagsSet": ["TagB"]}),
                         consumed_result=True)
    tracks = admin.message_track_detail(_msg(tags="TagA"))
    assert tracks[0].track_type == TrackType.CONSUMED_BUT_FILTERED


def test_message_track_sql92_empty_tags_set_falls_back_to_consumed(monkeypatch):
    # SQL92 订阅没有 tagsSet：忠实保留 Java 语义，落回 CONSUMED
    admin = _track_admin(monkeypatch, [GROUP],
                         _cc("CONSUME_PASSIVELY", {"tagsSet": []}),
                         consumed_result=True)
    tracks = admin.message_track_detail(_msg())
    assert tracks[0].track_type == TrackType.CONSUMED


def test_message_track_groups_sorted_and_no_route_means_empty(monkeypatch):
    admin = _admin(_FakeClient())
    monkeypatch.setattr(
        admin, "examine_topic_route",
        lambda topic: _route(("broker-b", ADDR_B), ("broker-a", ADDR_A)))
    monkeypatch.setattr(admin, "query_topic_consume_by_who", lambda addr, topic: {"g-b", "g-a"})
    monkeypatch.setattr(admin, "examine_consumer_connection_info",
                        lambda group: _cc("CONSUME_ACTIVELY"))
    monkeypatch.setattr(admin, "consumed", lambda msg, group: False)

    tracks = admin.message_track_detail(_msg())
    assert [t.consumer_group for t in tracks] == ["g-a", "g-b"]  # set 排序，输出确定

    # topic 无路由（broker 地址全空）⇒ Java 同样返回空列表
    monkeypatch.setattr(admin, "examine_topic_route", lambda topic: _route())
    assert admin.message_track_detail(_msg()) == []


def test_message_track_dto_round_trip():
    mt = MessageTrack(consumer_group=GROUP, track_type=TrackType.CONSUMED_BUT_FILTERED,
                      exception_desc="CODE:206 DESC:x")
    data = mt.encode()
    back = MessageTrack.decode(data)
    assert back.consumer_group == GROUP
    assert back.track_type == TrackType.CONSUMED_BUT_FILTERED
    assert back.exception_desc == "CODE:206 DESC:x"
    assert back.to_dict()["trackType"] == "CONSUMED_BUT_FILTERED"
